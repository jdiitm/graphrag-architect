from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, Set, Tuple

from orchestrator.app.redis_client import (
    create_async_redis,
    delete_keys_by_prefix,
    require_redis,
)

logger = logging.getLogger(__name__)


class EmbeddingProvider(Protocol):
    async def embed(self, text: str) -> List[float]: ...


@dataclass(frozen=True)
class CacheConfig:
    similarity_threshold: float = 0.92
    default_ttl_seconds: float = 300.0
    max_entries: int = 1024
    ttl_by_complexity: Optional[Dict[str, float]] = None


@dataclass
class CacheEntry:
    key_hash: str
    embedding: List[float]
    query: str
    result: Dict[str, Any]
    created_at: float
    ttl_seconds: float
    tenant_id: str = ""
    access_count: int = 0

    @property
    def is_expired(self) -> bool:
        return (time.monotonic() - self.created_at) > self.ttl_seconds


@dataclass(frozen=True)
class CacheStats:
    hits: int
    misses: int
    evictions: int
    size: int
    max_size: int


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    if len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _vectorized_cosine_similarity(a: List[float], b: List[float]) -> float:
    if len(a) != len(b):
        return 0.0
    try:
        import numpy as np
        arr_a = np.asarray(a, dtype=np.float64)
        arr_b = np.asarray(b, dtype=np.float64)
        norm_a = np.linalg.norm(arr_a)
        norm_b = np.linalg.norm(arr_b)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(arr_a, arr_b) / (norm_a * norm_b))
    except ImportError:
        return _cosine_similarity(a, b)


def _embedding_hash(embedding: List[float]) -> str:
    raw = "|".join(f"{v:.6f}" for v in embedding[:32])
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class _InflightRequest:
    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._failed = False

    async def wait(self) -> bool:
        await self._event.wait()
        return not self._failed

    def complete(self, failed: bool = False) -> None:
        self._failed = failed
        self._event.set()


class SemanticQueryCache:
    def __init__(self, config: CacheConfig | None = None) -> None:
        self._config = config or CacheConfig()
        self._entries: Dict[str, CacheEntry] = {}
        self._entry_generations: Dict[str, int] = {}
        self._generation = 0
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._inflight: Dict[str, _InflightRequest] = {}
        self._node_tags: Dict[str, Set[str]] = {}
        self._key_nodes: Dict[str, Set[str]] = {}

    @property
    def similarity_threshold(self) -> float:
        return self._config.similarity_threshold

    @property
    def generation(self) -> int:
        return self._generation

    def advance_generation(self) -> int:
        self._generation += 1
        return self._generation

    def has_inflight(self, key_hash: str) -> bool:
        return key_hash in self._inflight

    def get_inflight(self, key_hash: str) -> Optional[_InflightRequest]:
        return self._inflight.get(key_hash)

    def set_inflight(self, key_hash: str) -> _InflightRequest:
        request = _InflightRequest()
        self._inflight[key_hash] = request
        return request

    async def lookup_or_wait(
        self,
        query_embedding: List[float],
        tenant_id: str = "",
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        cached = self.lookup(query_embedding, tenant_id=tenant_id)
        if cached is not None:
            return cached, False

        key_hash = _embedding_hash(query_embedding)

        inflight = self.get_inflight(key_hash)
        if inflight is not None:
            succeeded = await inflight.wait()
            if succeeded:
                result = self.lookup(query_embedding, tenant_id=tenant_id)
                if result is not None:
                    return result, False
            return None, True

        self.set_inflight(key_hash)
        return None, True

    def notify_complete(
        self,
        query_embedding: List[float],
        failed: bool = False,
    ) -> None:
        key_hash = _embedding_hash(query_embedding)
        inflight = self._inflight.pop(key_hash, None)
        if inflight is not None:
            inflight.complete(failed=failed)

    def lookup(
        self,
        query_embedding: List[float],
        tenant_id: str = "",
    ) -> Optional[Dict[str, Any]]:
        self._evict_expired()
        best_entry: Optional[CacheEntry] = None
        best_sim = 0.0

        for entry in self._entries.values():
            if tenant_id and entry.tenant_id and entry.tenant_id != tenant_id:
                continue
            sim = _vectorized_cosine_similarity(query_embedding, entry.embedding)
            if sim >= self._config.similarity_threshold and sim > best_sim:
                best_sim = sim
                best_entry = entry

        if best_entry is not None:
            best_entry.access_count += 1
            self._hits += 1
            logger.debug(
                "Cache hit: query=%r similarity=%.4f", best_entry.query, best_sim,
            )
            return best_entry.result

        self._misses += 1
        return None

    def store(
        self,
        query: str,
        query_embedding: List[float],
        result: Dict[str, Any],
        tenant_id: str = "",
        complexity: str = "",
        node_ids: Optional[Set[str]] = None,
    ) -> None:
        self._evict_expired()
        self._enforce_max_size()

        ttl = self._config.default_ttl_seconds
        if complexity and self._config.ttl_by_complexity:
            ttl = self._config.ttl_by_complexity.get(complexity, ttl)

        key_hash = _embedding_hash(query_embedding)
        if key_hash in self._entries:
            self._remove_node_tags_for_key(key_hash)
        entry = CacheEntry(
            key_hash=key_hash,
            embedding=query_embedding,
            query=query,
            result=result,
            created_at=time.monotonic(),
            ttl_seconds=ttl,
            tenant_id=tenant_id,
        )
        self._entries[key_hash] = entry
        self._entry_generations[key_hash] = self._generation
        if node_ids:
            self._key_nodes[key_hash] = set(node_ids)
            for nid in node_ids:
                if nid not in self._node_tags:
                    self._node_tags[nid] = set()
                self._node_tags[nid].add(key_hash)

    def _remove_node_tags_for_key(self, key: str) -> None:
        nids = self._key_nodes.pop(key, None)
        if not nids:
            return
        for nid in nids:
            tag_set = self._node_tags.get(nid)
            if tag_set is not None:
                tag_set.discard(key)
                if not tag_set:
                    del self._node_tags[nid]

    def invalidate_tenant(self, tenant_id: str) -> int:
        to_remove = [
            k for k, v in self._entries.items() if v.tenant_id == tenant_id
        ]
        for k in to_remove:
            del self._entries[k]
            self._entry_generations.pop(k, None)
            self._remove_node_tags_for_key(k)
        return len(to_remove)

    def invalidate_by_nodes(self, node_ids: Set[str]) -> int:
        if not node_ids:
            return 0
        keys_to_remove: Set[str] = set()
        for nid in node_ids:
            tag_set = self._node_tags.get(nid)
            if tag_set:
                keys_to_remove.update(tag_set)
        for k in keys_to_remove:
            self._entries.pop(k, None)
            self._entry_generations.pop(k, None)
            self._remove_node_tags_for_key(k)
        return len(keys_to_remove)

    def invalidate_stale(self) -> int:
        stale = [
            k for k, gen in self._entry_generations.items()
            if gen < self._generation
        ]
        for k in stale:
            self._entries.pop(k, None)
            self._entry_generations.pop(k, None)
            self._remove_node_tags_for_key(k)
            self._evictions += 1
        return len(stale)

    def invalidate_all(self) -> int:
        count = len(self._entries)
        self._entries.clear()
        self._entry_generations.clear()
        self._node_tags.clear()
        self._key_nodes.clear()
        return count

    def stats(self) -> CacheStats:
        return CacheStats(
            hits=self._hits,
            misses=self._misses,
            evictions=self._evictions,
            size=len(self._entries),
            max_size=self._config.max_entries,
        )

    def _evict_expired(self) -> None:
        expired = [k for k, v in self._entries.items() if v.is_expired]
        for k in expired:
            del self._entries[k]
            self._entry_generations.pop(k, None)
            self._remove_node_tags_for_key(k)
            self._evictions += 1

    def _enforce_max_size(self) -> None:
        while len(self._entries) >= self._config.max_entries:
            oldest_key = min(
                self._entries, key=lambda k: self._entries[k].created_at,
            )
            del self._entries[oldest_key]
            self._entry_generations.pop(oldest_key, None)
            self._remove_node_tags_for_key(oldest_key)
            self._evictions += 1


class RedisSemanticQueryCache:
    def __init__(
        self,
        redis_url: str,
        config: CacheConfig | None = None,
        ttl_seconds: int = 300,
        key_prefix: str = "graphrag:semcache:",
        password: str = "",
        db: int = 0,
    ) -> None:
        require_redis("RedisSemanticQueryCache")
        self._redis = create_async_redis(redis_url, password=password, db=db)
        self._ttl = ttl_seconds
        self._prefix = key_prefix
        self._l1 = SemanticQueryCache(config=config)

    @property
    def generation(self) -> int:
        return self._l1.generation

    def advance_generation(self) -> int:
        return self._l1.advance_generation()

    async def invalidate_stale(self) -> int:
        count = self._l1.invalidate_stale()
        try:
            await delete_keys_by_prefix(self._redis, self._prefix)
        except Exception:
            logger.warning("Redis semantic cache invalidate_stale failed (non-fatal)")
        return count

    async def _redis_lookup(
        self,
        query_embedding: List[float],
        tenant_id: str = "",
    ) -> Optional[Dict[str, Any]]:
        try:
            cursor = None
            pattern = f"{self._prefix}*"
            threshold = self._l1.similarity_threshold
            best_result: Optional[Dict[str, Any]] = None
            best_sim = 0.0

            while cursor != 0:
                cursor, keys = await self._redis.scan(
                    cursor=cursor or 0, match=pattern, count=100,
                )
                for key in keys:
                    raw = await self._redis.get(key)
                    if raw is None:
                        continue
                    entry = json.loads(raw)
                    if tenant_id and entry.get("tenant_id") and entry["tenant_id"] != tenant_id:
                        continue
                    stored_emb = entry.get("embedding", [])
                    sim = _vectorized_cosine_similarity(query_embedding[:32], stored_emb)
                    if sim >= threshold and sim > best_sim:
                        best_sim = sim
                        best_result = entry.get("result")

            if best_result is not None:
                self._l1.store(
                    query=best_result.get("query", ""),
                    query_embedding=query_embedding,
                    result=best_result,
                    tenant_id=tenant_id,
                )
            return best_result
        except Exception:
            logger.warning("Redis semantic cache L2 lookup failed (non-fatal)")
            return None

    async def lookup_or_wait(
        self,
        query_embedding: List[float],
        tenant_id: str = "",
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        cached = self._l1.lookup(query_embedding, tenant_id=tenant_id)
        if cached is not None:
            return cached, False

        redis_hit = await self._redis_lookup(query_embedding, tenant_id=tenant_id)
        if redis_hit is not None:
            return redis_hit, False

        key_hash = _embedding_hash(query_embedding)
        inflight = self._l1.get_inflight(key_hash)
        if inflight is not None:
            succeeded = await inflight.wait()
            if succeeded:
                result = self._l1.lookup(query_embedding, tenant_id=tenant_id)
                if result is not None:
                    return result, False
            return None, True

        self._l1.set_inflight(key_hash)
        return None, True

    def notify_complete(
        self,
        query_embedding: List[float],
        failed: bool = False,
    ) -> None:
        self._l1.notify_complete(query_embedding, failed=failed)

    def lookup(
        self,
        query_embedding: List[float],
        tenant_id: str = "",
    ) -> Optional[Dict[str, Any]]:
        return self._l1.lookup(query_embedding, tenant_id=tenant_id)

    def store(
        self,
        query: str,
        query_embedding: List[float],
        result: Dict[str, Any],
        tenant_id: str = "",
        complexity: str = "",
        node_ids: Optional[Set[str]] = None,
    ) -> None:
        self._l1.store(
            query=query, query_embedding=query_embedding,
            result=result, tenant_id=tenant_id, complexity=complexity,
            node_ids=node_ids,
        )
        key_hash = _embedding_hash(query_embedding)
        try:
            payload = json.dumps({
                "query": query,
                "embedding": query_embedding[:32],
                "result": result,
                "tenant_id": tenant_id,
            }, default=str)
            loop = asyncio.get_running_loop()
            loop.create_task(
                self._redis.setex(
                    f"{self._prefix}{key_hash}", self._ttl, payload,
                )
            )
            if node_ids:
                for nid in node_ids:
                    tag_key = f"{self._prefix}nodetag:{nid}"
                    loop.create_task(self._redis.sadd(
                        tag_key, f"{self._prefix}{key_hash}",
                    ))
                    loop.create_task(self._redis.expire(
                        tag_key, self._ttl,
                    ))
        except RuntimeError:
            pass
        except Exception:
            logger.debug("Redis semantic cache store failed (non-fatal)")

    async def invalidate_by_nodes(self, node_ids: Set[str]) -> int:
        l1_removed = self._l1.invalidate_by_nodes(node_ids)
        try:
            keys_to_delete: set = set()
            tag_keys: list = []
            for nid in node_ids:
                tag_key = f"{self._prefix}nodetag:{nid}"
                tag_keys.append(tag_key)
                members = await self._redis.smembers(tag_key)
                keys_to_delete.update(members)
            if keys_to_delete:
                await self._redis.delete(*keys_to_delete)
            if tag_keys:
                await self._redis.delete(*tag_keys)
        except Exception:
            logger.warning("Redis node-level invalidation failed (non-fatal)")
        return l1_removed

    async def invalidate_tenant(self, tenant_id: str) -> int:
        l1_removed = self._l1.invalidate_tenant(tenant_id)
        try:
            cursor = None
            pattern = f"{self._prefix}*"
            keys_to_delete = []
            while cursor != 0:
                cursor, keys = await self._redis.scan(
                    cursor=cursor or 0, match=pattern, count=100,
                )
                for key in keys:
                    raw = await self._redis.get(key)
                    if raw is None:
                        continue
                    try:
                        entry = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    if entry.get("tenant_id") == tenant_id:
                        keys_to_delete.append(key)
            if keys_to_delete:
                await self._redis.delete(*keys_to_delete)
        except Exception:
            logger.warning("Redis tenant invalidation failed (non-fatal)")
        return l1_removed

    async def invalidate_all(self) -> int:
        count = self._l1.invalidate_all()
        try:
            await delete_keys_by_prefix(self._redis, self._prefix)
        except Exception:
            logger.debug("Redis semantic cache invalidate_all failed (non-fatal)")
        return count

    def stats(self) -> CacheStats:
        return self._l1.stats()


def create_semantic_cache(
    config: CacheConfig | None = None,
) -> Any:
    from orchestrator.app.config import RedisConfig
    redis_cfg = RedisConfig.from_env()
    if redis_cfg.url:
        return RedisSemanticQueryCache(
            redis_url=redis_cfg.url,
            password=redis_cfg.password,
            db=redis_cfg.db,
            config=config,
        )
    return SemanticQueryCache(config=config)
