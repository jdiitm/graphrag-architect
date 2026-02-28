from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, Set, Tuple

from orchestrator.app.redis_client import (
    create_async_redis,
    delete_keys_by_prefix,
    require_redis,
)

logger = logging.getLogger(__name__)


def compute_topology_hash(node_ids: Set[str]) -> str:
    if not node_ids:
        return hashlib.sha256(b"empty").hexdigest()[:16]
    canonical = "|".join(sorted(node_ids))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


class EmbeddingProvider(Protocol):
    async def embed(self, text: str) -> List[float]: ...


@dataclass(frozen=True)
class CacheConfig:
    similarity_threshold: float = 0.92
    default_ttl_seconds: float = 300.0
    max_entries: int = 1024
    ttl_by_complexity: Optional[Dict[str, float]] = None
    adaptive_threshold: bool = False
    min_threshold: float = 0.85
    max_threshold: float = 0.98
    density_sensitivity: float = 0.15
    hash_dimensions: Optional[int] = None


_NEUTRAL_MARGIN = 0.5


def compute_adaptive_threshold(
    best_sim: float,
    second_sim: Optional[float],
    base_threshold: float,
    min_threshold: float,
    max_threshold: float,
    density_sensitivity: float,
) -> float:
    if second_sim is None:
        return base_threshold
    margin = best_sim - second_sim
    adjustment = density_sensitivity * (_NEUTRAL_MARGIN - margin)
    effective = base_threshold + adjustment
    return max(min_threshold, min(max_threshold, effective))


@dataclass
class CacheEntry:
    key_hash: str
    embedding: List[float]
    query: str
    result: Dict[str, Any]
    created_at: float
    ttl_seconds: float
    tenant_id: str = ""
    acl_key: str = ""
    access_count: int = 0
    topology_hash: str = ""

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


def _embedding_hash(
    embedding: List[float],
    hash_dimensions: Optional[int] = None,
) -> str:
    dims = embedding[:hash_dimensions] if hash_dimensions is not None else embedding
    raw = "|".join(f"{v:.6f}" for v in dims)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


_FILLER_PATTERNS: List[re.Pattern[str]] = [
    re.compile(r"\bcan you tell me\b", re.IGNORECASE),
    re.compile(r"\bcould you tell me\b", re.IGNORECASE),
    re.compile(r"\bplease show me\b", re.IGNORECASE),
    re.compile(r"\bplease tell me\b", re.IGNORECASE),
    re.compile(r"\bshow me\b", re.IGNORECASE),
    re.compile(r"\btell me\b", re.IGNORECASE),
    re.compile(r"\bi want to know\b", re.IGNORECASE),
    re.compile(r"\bi would like to know\b", re.IGNORECASE),
    re.compile(r"\bcan you\b", re.IGNORECASE),
    re.compile(r"\bcould you\b", re.IGNORECASE),
    re.compile(r"\bplease\b", re.IGNORECASE),
]

_QUESTION_SYNONYMS: List[Tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bwhich\b", re.IGNORECASE), "what"),
    (re.compile(r"\bwhat are the\b", re.IGNORECASE), "what"),
    (re.compile(r"(?<!-)\bcalls\b(?!-)", re.IGNORECASE), "call"),
    (re.compile(r"(?<!-)\bservices\b(?!-)", re.IGNORECASE), "service"),
    (re.compile(r"(?<!-)\bdepends\b(?!-)", re.IGNORECASE), "depend"),
]

_WHITESPACE_COLLAPSE = re.compile(r"\s+")


def normalize_query(query: str) -> str:
    if not query or not query.strip():
        return ""
    text = query.strip().lower()
    text = re.sub(r"[?!.,;:]+$", "", text)
    for pattern in _FILLER_PATTERNS:
        text = pattern.sub("", text)
    for pattern, replacement in _QUESTION_SYNONYMS:
        text = pattern.sub(replacement, text)
    text = _WHITESPACE_COLLAPSE.sub(" ", text).strip()
    return text


@dataclass(frozen=True)
class CacheMetrics:
    hits: int
    misses: int
    evictions: int
    size: int

    @property
    def hit_ratio(self) -> float:
        total = self.hits + self.misses
        if total == 0:
            return 0.0
        return self.hits / total


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
        acl_key: str = "",
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        cached = self.lookup(query_embedding, tenant_id=tenant_id, acl_key=acl_key)
        if cached is not None:
            return cached, False

        hdim = self._config.hash_dimensions
        key_hash = _embedding_hash(query_embedding, hash_dimensions=hdim) + "|" + acl_key

        inflight = self.get_inflight(key_hash)
        if inflight is not None:
            succeeded = await inflight.wait()
            if succeeded:
                result = self.lookup(
                    query_embedding, tenant_id=tenant_id, acl_key=acl_key,
                )
                if result is not None:
                    return result, False
            return None, True

        self.set_inflight(key_hash)
        return None, True

    def notify_complete(
        self,
        query_embedding: List[float],
        failed: bool = False,
        acl_key: str = "",
    ) -> None:
        hdim = self._config.hash_dimensions
        key_hash = _embedding_hash(query_embedding, hash_dimensions=hdim) + "|" + acl_key
        inflight = self._inflight.pop(key_hash, None)
        if inflight is not None:
            inflight.complete(failed=failed)

    def lookup(
        self,
        query_embedding: List[float],
        tenant_id: str = "",
        acl_key: str = "",
    ) -> Optional[Dict[str, Any]]:
        self._evict_expired()
        best_sim = -1.0
        second_sim = -1.0
        best_entry: Optional[CacheEntry] = None
        tenant_entry_count = 0

        for entry in self._entries.values():
            if entry.tenant_id != tenant_id:
                continue
            if entry.acl_key != acl_key:
                continue
            tenant_entry_count += 1
            sim = _vectorized_cosine_similarity(query_embedding, entry.embedding)
            if sim > best_sim:
                second_sim = best_sim
                best_sim = sim
                best_entry = entry
            elif sim > second_sim:
                second_sim = sim

        if best_entry is None:
            self._misses += 1
            return None

        if self._config.adaptive_threshold:
            has_second = tenant_entry_count >= 2
            effective_threshold = compute_adaptive_threshold(
                best_sim=best_sim,
                second_sim=second_sim if has_second else None,
                base_threshold=self._config.similarity_threshold,
                min_threshold=self._config.min_threshold,
                max_threshold=self._config.max_threshold,
                density_sensitivity=self._config.density_sensitivity,
            )
        else:
            effective_threshold = self._config.similarity_threshold

        if best_sim >= effective_threshold:
            best_entry.access_count += 1
            self._hits += 1
            logger.debug(
                "Cache hit: query=%r similarity=%.4f threshold=%.4f",
                best_entry.query, best_sim, effective_threshold,
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
        acl_key: str = "",
    ) -> None:
        self._evict_expired()
        self._enforce_max_size()

        normalized = normalize_query(query)
        if normalized:
            query = normalized

        ttl = self._config.default_ttl_seconds
        if complexity and self._config.ttl_by_complexity:
            ttl = self._config.ttl_by_complexity.get(complexity, ttl)

        topo_hash = compute_topology_hash(node_ids) if node_ids else ""

        hdim = self._config.hash_dimensions
        key_hash = _embedding_hash(query_embedding, hash_dimensions=hdim) + "|" + acl_key
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
            acl_key=acl_key,
            topology_hash=topo_hash,
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

    def validate_topology(
        self,
        query_embedding: List[float],
        current_node_ids: Set[str],
        tenant_id: str = "",
        acl_key: str = "",
    ) -> bool:
        best_entry: Optional[CacheEntry] = None
        best_sim = -1.0
        for entry in self._entries.values():
            if tenant_id and entry.tenant_id != tenant_id:
                continue
            if entry.acl_key != acl_key:
                continue
            sim = _vectorized_cosine_similarity(query_embedding, entry.embedding)
            if sim > best_sim:
                best_sim = sim
                best_entry = entry
        if best_entry is None or not best_entry.topology_hash:
            return True
        return best_entry.topology_hash == compute_topology_hash(current_node_ids)

    def invalidate_stale_topologies(
        self, current_graph_node_ids: Set[str],
    ) -> int:
        stale_keys: List[str] = []
        for key, entry in self._entries.items():
            if not entry.topology_hash:
                continue
            cached_nodes = self._key_nodes.get(key, set())
            if not cached_nodes:
                continue
            if not cached_nodes.issubset(current_graph_node_ids):
                stale_keys.append(key)
        for k in stale_keys:
            self._entries.pop(k, None)
            self._entry_generations.pop(k, None)
            self._remove_node_tags_for_key(k)
            self._evictions += 1
        return len(stale_keys)

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

    def metrics(self) -> CacheMetrics:
        return CacheMetrics(
            hits=self._hits,
            misses=self._misses,
            evictions=self._evictions,
            size=len(self._entries),
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
        self._cache_config = config or CacheConfig()
        self._l1 = SemanticQueryCache(config=self._cache_config)

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
        acl_key: str = "",
    ) -> Optional[Dict[str, Any]]:
        try:
            hdim = self._cache_config.hash_dimensions
            key_hash = _embedding_hash(query_embedding, hash_dimensions=hdim) + "|" + acl_key
            redis_key = f"{self._prefix}{key_hash}"
            raw = await self._redis.get(redis_key)
            if raw is None:
                return None
            entry = json.loads(raw)
            if tenant_id and entry.get("tenant_id") and entry["tenant_id"] != tenant_id:
                return None
            if entry.get("acl_key", "") != acl_key:
                return None
            result = entry.get("result")
            if result is not None:
                self._l1.store(
                    query=entry.get("query", ""),
                    query_embedding=query_embedding,
                    result=result,
                    tenant_id=tenant_id,
                    acl_key=acl_key,
                )
            return result
        except Exception:
            logger.warning("Redis semantic cache L2 lookup failed (non-fatal)")
            return None

    async def lookup_or_wait(
        self,
        query_embedding: List[float],
        tenant_id: str = "",
        acl_key: str = "",
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        cached = self._l1.lookup(
            query_embedding, tenant_id=tenant_id, acl_key=acl_key,
        )
        if cached is not None:
            return cached, False

        redis_hit = await self._redis_lookup(
            query_embedding, tenant_id=tenant_id, acl_key=acl_key,
        )
        if redis_hit is not None:
            return redis_hit, False

        hdim = self._cache_config.hash_dimensions
        key_hash = _embedding_hash(query_embedding, hash_dimensions=hdim) + "|" + acl_key
        inflight = self._l1.get_inflight(key_hash)
        if inflight is not None:
            succeeded = await inflight.wait()
            if succeeded:
                result = self._l1.lookup(
                    query_embedding, tenant_id=tenant_id, acl_key=acl_key,
                )
                if result is not None:
                    return result, False
            return None, True

        self._l1.set_inflight(key_hash)
        return None, True

    def notify_complete(
        self,
        query_embedding: List[float],
        failed: bool = False,
        acl_key: str = "",
    ) -> None:
        self._l1.notify_complete(query_embedding, failed=failed, acl_key=acl_key)

    def lookup(
        self,
        query_embedding: List[float],
        tenant_id: str = "",
        acl_key: str = "",
    ) -> Optional[Dict[str, Any]]:
        return self._l1.lookup(
            query_embedding, tenant_id=tenant_id, acl_key=acl_key,
        )

    def store(
        self,
        query: str,
        query_embedding: List[float],
        result: Dict[str, Any],
        tenant_id: str = "",
        complexity: str = "",
        node_ids: Optional[Set[str]] = None,
        acl_key: str = "",
    ) -> None:
        self._l1.store(
            query=query, query_embedding=query_embedding,
            result=result, tenant_id=tenant_id, complexity=complexity,
            node_ids=node_ids, acl_key=acl_key,
        )
        hdim = self._cache_config.hash_dimensions
        key_hash = _embedding_hash(query_embedding, hash_dimensions=hdim) + "|" + acl_key
        payload_embedding = (
            query_embedding[:hdim] if hdim is not None else query_embedding
        )
        try:
            payload = json.dumps({
                "query": query,
                "embedding": payload_embedding,
                "result": result,
                "tenant_id": tenant_id,
                "acl_key": acl_key,
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
