from __future__ import annotations

import hashlib
import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol

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


class SemanticQueryCache:
    def __init__(self, config: CacheConfig | None = None) -> None:
        self._config = config or CacheConfig()
        self._entries: Dict[str, CacheEntry] = {}
        self._hits = 0
        self._misses = 0
        self._evictions = 0

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

    def invalidate_tenant(self, tenant_id: str) -> int:
        to_remove = [
            k for k, v in self._entries.items() if v.tenant_id == tenant_id
        ]
        for k in to_remove:
            del self._entries[k]
        return len(to_remove)

    def invalidate_all(self) -> int:
        count = len(self._entries)
        self._entries.clear()
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
            self._evictions += 1

    def _enforce_max_size(self) -> None:
        while len(self._entries) >= self._config.max_entries:
            oldest_key = min(
                self._entries, key=lambda k: self._entries[k].created_at,
            )
            del self._entries[oldest_key]
            self._evictions += 1
