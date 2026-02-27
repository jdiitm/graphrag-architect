from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.semantic_cache import (
    CacheConfig,
    CacheEntry,
    RedisSemanticQueryCache,
    SemanticQueryCache,
    _cosine_similarity,
    _embedding_hash,
    _vectorized_cosine_similarity,
    compute_adaptive_threshold,
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


class TestVectorizedCosineSimilarity:

    def test_identical_vectors(self) -> None:
        v = [1.0, 0.0, 0.0]
        assert _vectorized_cosine_similarity(v, v) == pytest.approx(1.0)

    def test_orthogonal_vectors(self) -> None:
        a = [1.0, 0.0]
        b = [0.0, 1.0]
        assert _vectorized_cosine_similarity(a, b) == pytest.approx(0.0)

    def test_zero_vector(self) -> None:
        assert _vectorized_cosine_similarity([0.0, 0.0], [1.0, 0.0]) == 0.0

    def test_different_lengths_returns_zero(self) -> None:
        assert _vectorized_cosine_similarity([1.0], [1.0, 2.0]) == 0.0

    def test_matches_naive_implementation(self) -> None:
        a = [0.3, 0.7, 0.1, 0.5]
        b = [0.6, 0.2, 0.9, 0.4]
        naive_result = _cosine_similarity(a, b)
        vectorized_result = _vectorized_cosine_similarity(a, b)
        assert vectorized_result == pytest.approx(naive_result, abs=1e-7)

    def test_high_dimensional_performance(self) -> None:
        import random
        random.seed(42)
        dim = 1536
        a = [random.gauss(0, 1) for _ in range(dim)]
        b = [random.gauss(0, 1) for _ in range(dim)]
        result = _vectorized_cosine_similarity(a, b)
        assert -1.0 <= result <= 1.0


class TestSemanticCacheUsesVectorizedLookup:

    def test_lookup_uses_vectorized_similarity(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "test"})
        result = cache.lookup(embedding)
        assert result == {"answer": "test"}

    def test_bulk_lookup_performance_scales(self) -> None:
        cache = SemanticQueryCache(CacheConfig(
            similarity_threshold=0.95, max_entries=512,
        ))
        for i in range(100):
            emb = [0.0] * 128
            emb[i % 128] = 1.0
            cache.store(f"q{i}", emb, {"idx": i})

        query_emb = [0.0] * 128
        query_emb[0] = 1.0
        result = cache.lookup(query_emb)
        assert result is not None


class TestRedisSemanticQueryCacheInvalidateTenant:

    @pytest.mark.asyncio
    async def test_invalidate_tenant_purges_l1_and_redis(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.scan = AsyncMock(return_value=(0, [
            b"graphrag:semcache:abc123",
            b"graphrag:semcache:def456",
        ]))
        mock_redis.get = AsyncMock(side_effect=[
            json.dumps({"tenant_id": "t1", "result": {}, "embedding": [0.1], "query": "q1"}),
            json.dumps({"tenant_id": "t2", "result": {}, "embedding": [0.2], "query": "q2"}),
        ])
        mock_redis.delete = AsyncMock()

        with patch(
            "orchestrator.app.semantic_cache.create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379", config=CacheConfig(similarity_threshold=0.9),
            )
            cache._l1.store("q1", [0.1] * 32, {"a": 1}, tenant_id="t1")
            cache._l1.store("q2", [0.2] * 32, {"a": 2}, tenant_id="t2")

            removed = await cache.invalidate_tenant("t1")

        assert removed >= 1
        assert cache._l1.stats().size == 1
        mock_redis.delete.assert_called()

    @pytest.mark.asyncio
    async def test_invalidate_tenant_only_deletes_matching_tenant_keys(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.scan = AsyncMock(return_value=(0, [
            b"graphrag:semcache:key1",
            b"graphrag:semcache:key2",
        ]))
        mock_redis.get = AsyncMock(side_effect=[
            json.dumps({"tenant_id": "target", "result": {}, "embedding": [0.1], "query": "q"}),
            json.dumps({"tenant_id": "other", "result": {}, "embedding": [0.2], "query": "q"}),
        ])
        mock_redis.delete = AsyncMock()

        with patch(
            "orchestrator.app.semantic_cache.create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379", config=CacheConfig(similarity_threshold=0.9),
            )
            await cache.invalidate_tenant("target")

        delete_calls = mock_redis.delete.call_args_list
        deleted_keys = []
        for call in delete_calls:
            deleted_keys.extend(call[0])
        assert b"graphrag:semcache:key1" in deleted_keys
        assert b"graphrag:semcache:key2" not in deleted_keys

    @pytest.mark.asyncio
    async def test_invalidate_tenant_tolerates_redis_failure(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.scan = AsyncMock(side_effect=ConnectionError("redis down"))

        with patch(
            "orchestrator.app.semantic_cache.create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379", config=CacheConfig(similarity_threshold=0.9),
            )
            cache._l1.store("q1", [0.1] * 32, {"a": 1}, tenant_id="t1")
            removed = await cache.invalidate_tenant("t1")

        assert removed >= 1


class TestSemanticQueryCacheNodeLevelInvalidation:

    def test_invalidate_by_nodes_removes_tagged_entries(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.store("q1", [1.0, 0.0], {"a": 1}, tenant_id="t1", node_ids={"n1", "n2"})
        cache.store("q2", [0.0, 1.0], {"a": 2}, tenant_id="t1", node_ids={"n3"})
        removed = cache.invalidate_by_nodes({"n1"})
        assert removed == 1
        assert cache.stats().size == 1

    def test_invalidate_by_nodes_preserves_unrelated_entries(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.store("q1", [1.0, 0.0], {"a": 1}, tenant_id="t1", node_ids={"n1"})
        cache.store("q2", [0.0, 1.0], {"a": 2}, tenant_id="t1", node_ids={"n2"})
        cache.invalidate_by_nodes({"n1"})
        assert cache.lookup([0.0, 1.0], tenant_id="t1") == {"a": 2}

    def test_invalidate_by_nodes_handles_overlapping_tags(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.store("q1", [1.0, 0.0], {"a": 1}, node_ids={"n1", "n2"})
        cache.store("q2", [0.0, 1.0], {"a": 2}, node_ids={"n2", "n3"})
        removed = cache.invalidate_by_nodes({"n2"})
        assert removed == 2
        assert cache.stats().size == 0

    def test_invalidate_by_nodes_empty_set_is_noop(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.store("q1", [1.0, 0.0], {"a": 1}, node_ids={"n1"})
        removed = cache.invalidate_by_nodes(set())
        assert removed == 0
        assert cache.stats().size == 1

    def test_invalidate_by_nodes_unknown_nodes_is_noop(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.store("q1", [1.0, 0.0], {"a": 1}, node_ids={"n1"})
        removed = cache.invalidate_by_nodes({"n99"})
        assert removed == 0
        assert cache.stats().size == 1

    def test_store_without_node_ids_is_not_affected_by_invalidate_by_nodes(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.store("q1", [1.0, 0.0], {"a": 1})
        removed = cache.invalidate_by_nodes({"n1"})
        assert removed == 0
        assert cache.stats().size == 1


class TestRedisSemanticCacheNodeLevelInvalidation:

    @pytest.mark.asyncio
    async def test_invalidate_by_nodes_clears_l1_and_redis_tags(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.smembers = AsyncMock(return_value={b"graphrag:semcache:hash1"})
        mock_redis.delete = AsyncMock()

        with patch(
            "orchestrator.app.semantic_cache.create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379",
                config=CacheConfig(similarity_threshold=0.9),
            )
            cache._l1.store(
                "q1", [0.1] * 32, {"a": 1}, tenant_id="t1",
                node_ids={"n1"},
            )
            removed = await cache.invalidate_by_nodes({"n1"})

        assert removed >= 1

    @pytest.mark.asyncio
    async def test_invalidate_by_nodes_tolerates_redis_failure(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.smembers = AsyncMock(side_effect=ConnectionError("redis down"))

        with patch(
            "orchestrator.app.semantic_cache.create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379",
                config=CacheConfig(similarity_threshold=0.9),
            )
            cache._l1.store(
                "q1", [0.1] * 32, {"a": 1}, tenant_id="t1",
                node_ids={"n1"},
            )
            removed = await cache.invalidate_by_nodes({"n1"})

        assert removed >= 1


class TestRedisSemanticCacheNodetagTTL:

    @pytest.mark.asyncio
    async def test_store_with_node_ids_sets_expire_on_tag_keys(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.setex = AsyncMock()
        mock_redis.sadd = AsyncMock()
        mock_redis.expire = AsyncMock()

        with patch(
            "orchestrator.app.semantic_cache.create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            ttl = 600
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379",
                config=CacheConfig(similarity_threshold=0.9),
                ttl_seconds=ttl,
            )
            cache.store(
                "q1", [0.1] * 32, {"a": 1},
                tenant_id="t1", node_ids={"n1", "n2"},
            )
            await asyncio.sleep(0)

        expire_calls = mock_redis.expire.call_args_list
        expired_keys = {call.args[0] for call in expire_calls}
        expired_ttls = {call.args[1] for call in expire_calls}

        prefix = cache._prefix
        assert f"{prefix}nodetag:n1" in expired_keys
        assert f"{prefix}nodetag:n2" in expired_keys
        assert expired_ttls == {ttl}


def _unit_vector(dim: int, index: int) -> list[float]:
    v = [0.0] * dim
    v[index] = 1.0
    return v


def _perturbed_vector(base: list[float], noise: float) -> list[float]:
    import math
    result = [b + noise * (0.01 * i) for i, b in enumerate(base)]
    norm = math.sqrt(sum(x * x for x in result))
    return [x / norm for x in result] if norm > 0 else result


class TestComputeAdaptiveThreshold:

    def test_no_second_neighbor_returns_base_threshold(self) -> None:
        result = compute_adaptive_threshold(
            best_sim=0.95,
            second_sim=None,
            base_threshold=0.92,
            min_threshold=0.85,
            max_threshold=0.98,
            density_sensitivity=0.15,
        )
        assert result == pytest.approx(0.92)

    def test_wide_margin_lowers_toward_min(self) -> None:
        result = compute_adaptive_threshold(
            best_sim=0.96,
            second_sim=0.30,
            base_threshold=0.92,
            min_threshold=0.85,
            max_threshold=0.98,
            density_sensitivity=0.15,
        )
        assert result < 0.92
        assert result >= 0.85

    def test_tight_margin_raises_toward_max(self) -> None:
        result = compute_adaptive_threshold(
            best_sim=0.95,
            second_sim=0.94,
            base_threshold=0.92,
            min_threshold=0.85,
            max_threshold=0.98,
            density_sensitivity=0.15,
        )
        assert result > 0.92
        assert result <= 0.98

    def test_never_exceeds_max_threshold(self) -> None:
        result = compute_adaptive_threshold(
            best_sim=0.99,
            second_sim=0.99,
            base_threshold=0.92,
            min_threshold=0.85,
            max_threshold=0.98,
            density_sensitivity=1.0,
        )
        assert result <= 0.98

    def test_never_drops_below_min_threshold(self) -> None:
        result = compute_adaptive_threshold(
            best_sim=0.99,
            second_sim=0.01,
            base_threshold=0.92,
            min_threshold=0.85,
            max_threshold=0.98,
            density_sensitivity=1.0,
        )
        assert result >= 0.85


class TestAdaptiveSemanticCacheLookup:

    def test_backward_compat_static_mode(self) -> None:
        config = CacheConfig(similarity_threshold=0.9)
        cache = SemanticQueryCache(config=config)
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "static"})
        result = cache.lookup(embedding)
        assert result == {"answer": "static"}

    def test_adaptive_enabled_still_hits_identical(self) -> None:
        config = CacheConfig(
            similarity_threshold=0.92,
            adaptive_threshold=True,
            min_threshold=0.85,
            max_threshold=0.98,
            density_sensitivity=0.15,
        )
        cache = SemanticQueryCache(config=config)
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "adaptive"})
        result = cache.lookup(embedding)
        assert result == {"answer": "adaptive"}

    def test_adaptive_dense_region_rejects_marginal_hit(self) -> None:
        import math

        config = CacheConfig(
            similarity_threshold=0.90,
            adaptive_threshold=True,
            min_threshold=0.85,
            max_threshold=0.98,
            density_sensitivity=0.20,
        )
        cache = SemanticQueryCache(config=config)

        def _normalize(v: list[float]) -> list[float]:
            n = math.sqrt(sum(x * x for x in v))
            return [x / n for x in v] if n > 0 else v

        entry_a = _normalize([0.90, 0.44, 0.0])
        entry_b = _normalize([0.90, 0.0, 0.44])
        entry_c = _normalize([0.85, 0.37, 0.37])

        cache.store("qa", entry_a, {"a": "A"})
        cache.store("qb", entry_b, {"a": "B"})
        cache.store("qc", entry_c, {"a": "C"})

        query = _normalize([0.95, 0.25, 0.19])

        sims = sorted([
            _cosine_similarity(query, entry_a),
            _cosine_similarity(query, entry_b),
            _cosine_similarity(query, entry_c),
        ], reverse=True)
        margin = sims[0] - sims[1]
        assert margin < 0.10

        static_cache = SemanticQueryCache(
            config=CacheConfig(similarity_threshold=0.90),
        )
        for key in cache._entries:
            entry = cache._entries[key]
            static_cache.store(entry.query, entry.embedding, entry.result)

        static_result = static_cache.lookup(query)
        adaptive_result = cache.lookup(query)
        assert static_result is not None
        assert adaptive_result is None

    def test_adaptive_sparse_region_accepts_looser_match(self) -> None:
        config = CacheConfig(
            similarity_threshold=0.95,
            adaptive_threshold=True,
            min_threshold=0.85,
            max_threshold=0.98,
            density_sensitivity=0.15,
        )
        cache = SemanticQueryCache(config=config)
        cache.store("q1", _unit_vector(8, 0), {"a": 1})
        cache.store("q2", _unit_vector(8, 7), {"a": 2})

        query = [0.93, 0.37, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        import math
        norm = math.sqrt(sum(x * x for x in query))
        query = [x / norm for x in query]

        sim_to_stored = _cosine_similarity(query, _unit_vector(8, 0))
        assert sim_to_stored < 0.95
        assert sim_to_stored >= 0.85

        result = cache.lookup(query)
        assert result == {"a": 1}

    def test_adaptive_single_entry_uses_base_threshold(self) -> None:
        config = CacheConfig(
            similarity_threshold=0.92,
            adaptive_threshold=True,
            min_threshold=0.85,
            max_threshold=0.98,
            density_sensitivity=0.15,
        )
        cache = SemanticQueryCache(config=config)
        cache.store("q1", [1.0, 0.0, 0.0], {"a": 1})

        result = cache.lookup([0.999, 0.01, 0.0])
        assert result == {"a": 1}

        dissimilar = [0.7, 0.7, 0.1]
        result_miss = cache.lookup(dissimilar)
        assert result_miss is None
