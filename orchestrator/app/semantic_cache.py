from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, Tuple

from orchestrator.app.redis_client import create_async_redis, require_redis

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

    @property
    def generation(self) -> int:
        return self._generation

    def advance_generation(self) -> int:
        self._generation += 1
        return self._generation

    async def lookup_or_wait(
        self,
        query_embedding: List[float],
        tenant_id: str = "",
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        cached = self.lookup(query_embedding, tenant_id=tenant_id)
        if cached is not None:
            return cached, False

        key_hash = _embedding_hash(query_embedding)

        if key_hash in self._inflight:
            inflight = self._inflight[key_hash]
            succeeded = await inflight.wait()
            if succeeded:
                result = self.lookup(query_embedding, tenant_id=tenant_id)
                if result is not None:
                    return result, False
            return None, True

        self._inflight[key_hash] = _InflightRequest()
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
            sim = _cosine_similarity(query_embedding, entry.embedding)
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
    ) -> None:
        self._evict_expired()
        self._enforce_max_size()

        ttl = self._config.default_ttl_seconds
        if complexity and self._config.ttl_by_complexity:
            ttl = self._config.ttl_by_complexity.get(complexity, ttl)

        key_hash = _embedding_hash(query_embedding)
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

    def invalidate_tenant(self, tenant_id: str) -> int:
        to_remove = [
            k for k, v in self._entries.items() if v.tenant_id == tenant_id
        ]
        for k in to_remove:
            del self._entries[k]
            self._entry_generations.pop(k, None)
        return len(to_remove)

    def invalidate_stale(self) -> int:
        stale = [
            k for k, gen in self._entry_generations.items()
            if gen < self._generation
        ]
        for k in stale:
            self._entries.pop(k, None)
            self._entry_generations.pop(k, None)
            self._evictions += 1
        return len(stale)

    def invalidate_all(self) -> int:
        count = len(self._entries)
        self._entries.clear()
        self._entry_generations.clear()
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
            self._evictions += 1

    def _enforce_max_size(self) -> None:
        while len(self._entries) >= self._config.max_entries:
            oldest_key = min(
                self._entries, key=lambda k: self._entries[k].created_at,
            )
            del self._entries[oldest_key]
            self._entry_generations.pop(oldest_key, None)
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

    def invalidate_stale(self) -> int:
        return self._l1.invalidate_stale()

    async def lookup_or_wait(
        self,
        query_embedding: List[float],
        tenant_id: str = "",
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        return await self._l1.lookup_or_wait(query_embedding, tenant_id=tenant_id)

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
    ) -> None:
        self._l1.store(
            query=query, query_embedding=query_embedding,
            result=result, tenant_id=tenant_id, complexity=complexity,
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
        except RuntimeError:
            pass
        except Exception:
            logger.debug("Redis semantic cache store failed (non-fatal)")

    def invalidate_tenant(self, tenant_id: str) -> int:
        return self._l1.invalidate_tenant(tenant_id)

    async def invalidate_all(self) -> int:
        count = self._l1.invalidate_all()
        try:
            cursor = None
            pattern = f"{self._prefix}*"
            while cursor != 0:
                cursor, keys = await self._redis.scan(
                    cursor=cursor or 0, match=pattern, count=100,
                )
                if keys:
                    await self._redis.delete(*keys)
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
