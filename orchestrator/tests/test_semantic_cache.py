from __future__ import annotations

import asyncio
import time
from unittest.mock import patch

import pytest

from orchestrator.app.semantic_cache import (
    CacheConfig,
    CacheEntry,
    SemanticQueryCache,
    _cosine_similarity,
    _embedding_hash,
)


class TestCosineSimilarity:

    def test_identical_vectors(self) -> None:
        v = [1.0, 0.0, 0.0]
        assert _cosine_similarity(v, v) == pytest.approx(1.0)

    def test_orthogonal_vectors(self) -> None:
        a = [1.0, 0.0]
        b = [0.0, 1.0]
        assert _cosine_similarity(a, b) == pytest.approx(0.0)

    def test_different_lengths(self) -> None:
        assert _cosine_similarity([1.0], [1.0, 2.0]) == 0.0

    def test_zero_vector(self) -> None:
        assert _cosine_similarity([0.0, 0.0], [1.0, 0.0]) == 0.0


class TestEmbeddingHash:

    def test_deterministic(self) -> None:
        e = [0.1, 0.2, 0.3]
        assert _embedding_hash(e) == _embedding_hash(e)

    def test_different_embeddings_differ(self) -> None:
        assert _embedding_hash([0.1, 0.2]) != _embedding_hash([0.3, 0.4])


class TestCacheEntry:

    def test_not_expired_within_ttl(self) -> None:
        entry = CacheEntry(
            key_hash="abc",
            embedding=[1.0],
            query="test",
            result={"answer": "x"},
            created_at=time.monotonic(),
            ttl_seconds=60.0,
        )
        assert not entry.is_expired

    def test_expired_after_ttl(self) -> None:
        entry = CacheEntry(
            key_hash="abc",
            embedding=[1.0],
            query="test",
            result={"answer": "x"},
            created_at=time.monotonic() - 120,
            ttl_seconds=60.0,
        )
        assert entry.is_expired


class TestSemanticQueryCacheLookup:

    def test_miss_on_empty_cache(self) -> None:
        cache = SemanticQueryCache()
        result = cache.lookup([1.0, 0.0, 0.0])
        assert result is None

    def test_hit_on_identical_embedding(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        cache.store("what services call auth?", embedding, {"answer": "gateway"})
        result = cache.lookup(embedding)
        assert result == {"answer": "gateway"}

    def test_hit_on_similar_embedding(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.95))
        cache.store("auth dependencies", [1.0, 0.0, 0.0], {"answer": "db"})
        similar = [0.99, 0.01, 0.0]
        result = cache.lookup(similar)
        assert result == {"answer": "db"}

    def test_miss_on_dissimilar_embedding(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.95))
        cache.store("auth services", [1.0, 0.0, 0.0], {"answer": "db"})
        dissimilar = [0.0, 1.0, 0.0]
        result = cache.lookup(dissimilar)
        assert result is None


class TestSemanticQueryCacheTTL:

    def test_expired_entry_not_returned(self) -> None:
        cache = SemanticQueryCache(CacheConfig(
            similarity_threshold=0.9, default_ttl_seconds=1.0,
        ))
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "a"})

        key = list(cache._entries.keys())[0]
        cache._entries[key].created_at = time.monotonic() - 10

        result = cache.lookup(embedding)
        assert result is None

    def test_complexity_based_ttl(self) -> None:
        config = CacheConfig(
            default_ttl_seconds=60.0,
            ttl_by_complexity={"entity_lookup": 600.0, "multi_hop": 30.0},
        )
        cache = SemanticQueryCache(config)
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "a"}, complexity="entity_lookup")
        entry = list(cache._entries.values())[0]
        assert entry.ttl_seconds == 600.0


class TestSemanticQueryCacheInvalidation:

    def test_invalidate_tenant(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.store("q1", [1.0, 0.0], {"a": 1}, tenant_id="t1")
        cache.store("q2", [0.0, 1.0], {"a": 2}, tenant_id="t2")
        removed = cache.invalidate_tenant("t1")
        assert removed == 1
        assert cache.stats().size == 1

    def test_invalidate_all(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.store("q1", [1.0, 0.0], {"a": 1})
        cache.store("q2", [0.0, 1.0], {"a": 2})
        removed = cache.invalidate_all()
        assert removed == 2
        assert cache.stats().size == 0


class TestSemanticQueryCacheTenantIsolation:

    def test_tenant_scoped_lookup(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"a": "tenant1"}, tenant_id="t1")
        assert cache.lookup(embedding, tenant_id="t1") == {"a": "tenant1"}
        assert cache.lookup(embedding, tenant_id="t2") is None


class TestSemanticQueryCacheEviction:

    def test_max_entries_enforced(self) -> None:
        cache = SemanticQueryCache(CacheConfig(
            max_entries=2, similarity_threshold=0.9,
        ))
        cache.store("q1", [1.0, 0.0], {"a": 1})
        cache.store("q2", [0.0, 1.0], {"a": 2})
        cache.store("q3", [0.5, 0.5], {"a": 3})
        assert cache.stats().size == 2
        assert cache.stats().evictions >= 1


class TestSemanticQueryCacheStats:

    def test_stats_tracking(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        cache.lookup(embedding)
        cache.store("q", embedding, {"a": 1})
        cache.lookup(embedding)

        stats = cache.stats()
        assert stats.misses == 1
        assert stats.hits == 1
        assert stats.size == 1


class TestSingleflightCoalescing:

    @pytest.mark.asyncio
    async def test_owner_gets_none_and_is_owner_true(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        result, is_owner = await cache.lookup_or_wait(embedding)
        assert result is None
        assert is_owner is True
        cache.notify_complete(embedding)

    @pytest.mark.asyncio
    async def test_cache_hit_returns_result_and_not_owner(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "cached"})
        result, is_owner = await cache.lookup_or_wait(embedding)
        assert result == {"answer": "cached"}
        assert is_owner is False

    @pytest.mark.asyncio
    async def test_waiter_receives_result_after_owner_completes(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]

        r1, owner1 = await cache.lookup_or_wait(embedding)
        assert r1 is None and owner1 is True

        async def waiter():
            return await cache.lookup_or_wait(embedding)

        task = asyncio.create_task(waiter())
        await asyncio.sleep(0.01)

        cache.store("q", embedding, {"answer": "computed"})
        cache.notify_complete(embedding)

        r2, owner2 = await task
        assert r2 == {"answer": "computed"}
        assert owner2 is False

    @pytest.mark.asyncio
    async def test_waiter_becomes_owner_on_failure(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]

        r1, owner1 = await cache.lookup_or_wait(embedding)
        assert r1 is None and owner1 is True

        async def waiter():
            return await cache.lookup_or_wait(embedding)

        task = asyncio.create_task(waiter())
        await asyncio.sleep(0.01)

        cache.notify_complete(embedding, failed=True)

        r2, owner2 = await task
        assert r2 is None
        assert owner2 is True
        cache.notify_complete(embedding)
