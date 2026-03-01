from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.semantic_cache import (
    CacheConfig,
    RedisSemanticQueryCache,
    SemanticQueryCache,
)


def _unit_embedding(dim: int, index: int) -> list[float]:
    v = [0.0] * dim
    v[index % dim] = 1.0
    return v


class TestIndexedLookupReplacesLinearScan:

    def test_correct_results_with_many_entries(self) -> None:
        config = CacheConfig(
            similarity_threshold=0.9, max_entries=2048,
        )
        cache = SemanticQueryCache(config=config)
        for i in range(150):
            emb = _unit_embedding(64, i)
            cache.store(
                f"query-{i}", emb, {"index": i},
                tenant_id="t1",
            )
        query_emb = _unit_embedding(64, 5)
        result = cache.lookup(query_emb, tenant_id="t1")
        assert result is not None

    def test_tenant_acl_index_exists_and_populated(self) -> None:
        cache = SemanticQueryCache(
            CacheConfig(similarity_threshold=0.9),
        )
        cache.store(
            "q1", [1.0, 0.0, 0.0], {"a": 1},
            tenant_id="t1", acl_key="admin",
        )
        cache.store(
            "q2", [0.0, 1.0, 0.0], {"a": 2},
            tenant_id="t1", acl_key="read",
        )
        cache.store(
            "q3", [0.0, 0.0, 1.0], {"a": 3},
            tenant_id="t2", acl_key="admin",
        )
        assert hasattr(cache, "_tenant_acl_index")
        assert ("t1", "admin") in cache._tenant_acl_index
        assert ("t1", "read") in cache._tenant_acl_index
        assert ("t2", "admin") in cache._tenant_acl_index
        assert len(cache._tenant_acl_index[("t1", "admin")]) == 1
        assert len(cache._tenant_acl_index[("t1", "read")]) == 1
        assert len(cache._tenant_acl_index[("t2", "admin")]) == 1

    def test_lookup_scoped_to_matching_tenant_acl(self) -> None:
        config = CacheConfig(
            similarity_threshold=0.9, max_entries=2048,
        )
        cache = SemanticQueryCache(config=config)
        for i in range(100):
            emb = _unit_embedding(32, i)
            cache.store(
                f"t1-q{i}", emb,
                {"tenant": "t1", "i": i},
                tenant_id="t1",
            )
        for i in range(100):
            emb = [0.001] * 32
            emb[i % 32] += 1.0
            cache.store(
                f"t2-q{i}", emb,
                {"tenant": "t2", "i": i},
                tenant_id="t2",
            )
        query_emb = _unit_embedding(32, 0)
        result = cache.lookup(query_emb, tenant_id="t1")
        assert result is not None
        assert result["tenant"] == "t1"


class TestIndexMaintainedThroughLifecycle:

    def test_store_invalidate_store_keeps_index_in_sync(
        self,
    ) -> None:
        cache = SemanticQueryCache(
            CacheConfig(similarity_threshold=0.9),
        )
        cache.store(
            "q1", [1.0, 0.0, 0.0], {"a": 1},
            tenant_id="t1", node_ids={"n1"},
        )
        cache.store(
            "q2", [0.0, 1.0, 0.0], {"a": 2},
            tenant_id="t1", node_ids={"n2"},
        )
        assert cache.lookup(
            [1.0, 0.0, 0.0], tenant_id="t1",
        ) == {"a": 1}
        assert cache.lookup(
            [0.0, 1.0, 0.0], tenant_id="t1",
        ) == {"a": 2}

        cache.invalidate_by_nodes({"n1"})

        assert cache.lookup(
            [1.0, 0.0, 0.0], tenant_id="t1",
        ) is None
        assert cache.lookup(
            [0.0, 1.0, 0.0], tenant_id="t1",
        ) == {"a": 2}

        assert hasattr(cache, "_tenant_acl_index")
        scope_entries = cache._tenant_acl_index.get(
            ("t1", ""), set(),
        )
        assert len(scope_entries) == 1

        cache.store(
            "q3", [0.0, 0.0, 1.0], {"a": 3},
            tenant_id="t1", node_ids={"n3"},
        )
        assert cache.lookup(
            [0.0, 0.0, 1.0], tenant_id="t1",
        ) == {"a": 3}

        scope_entries = cache._tenant_acl_index.get(
            ("t1", ""), set(),
        )
        assert len(scope_entries) == 2

    def test_invalidate_tenant_clears_index(self) -> None:
        cache = SemanticQueryCache(
            CacheConfig(similarity_threshold=0.9),
        )
        cache.store(
            "q1", [1.0, 0.0], {"a": 1}, tenant_id="t1",
        )
        cache.store(
            "q2", [0.0, 1.0], {"a": 2}, tenant_id="t1",
        )
        cache.store(
            "q3", [0.5, 0.5], {"a": 3}, tenant_id="t2",
        )

        assert hasattr(cache, "_tenant_acl_index")
        assert len(
            cache._tenant_acl_index.get(("t1", ""), set()),
        ) == 2

        cache.invalidate_tenant("t1")

        assert len(
            cache._tenant_acl_index.get(("t1", ""), set()),
        ) == 0
        assert len(
            cache._tenant_acl_index.get(("t2", ""), set()),
        ) == 1

    def test_invalidate_all_clears_entire_index(self) -> None:
        cache = SemanticQueryCache(
            CacheConfig(similarity_threshold=0.9),
        )
        cache.store(
            "q1", [1.0, 0.0], {"a": 1}, tenant_id="t1",
        )
        cache.store(
            "q2", [0.0, 1.0], {"a": 2}, tenant_id="t2",
        )

        assert hasattr(cache, "_tenant_acl_index")
        cache.invalidate_all()
        assert len(cache._tenant_acl_index) == 0

    def test_max_size_eviction_updates_index(self) -> None:
        cache = SemanticQueryCache(CacheConfig(
            similarity_threshold=0.9, max_entries=3,
        ))
        cache.store(
            "q1", [1.0, 0.0], {"a": 1}, tenant_id="t1",
        )
        cache.store(
            "q2", [0.0, 1.0], {"a": 2}, tenant_id="t1",
        )
        cache.store(
            "q3", [0.5, 0.5], {"a": 3}, tenant_id="t1",
        )
        cache.store(
            "q4", [0.7, 0.7], {"a": 4}, tenant_id="t1",
        )

        assert hasattr(cache, "_tenant_acl_index")
        scope_entries = cache._tenant_acl_index.get(
            ("t1", ""), set(),
        )
        assert len(scope_entries) == 3


class TestRedisInvalidationUsesStreamPublish:

    @pytest.mark.asyncio
    async def test_invalidate_by_nodes_publishes_to_stream(
        self,
    ) -> None:
        mock_redis = AsyncMock()

        with patch(
            "orchestrator.app.semantic_cache"
            ".create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379",
                config=CacheConfig(similarity_threshold=0.9),
            )
            cache._l1.store(
                "q1", [0.1] * 32, {"a": 1},
                tenant_id="t1", node_ids={"n1", "n2"},
            )
            await cache.invalidate_by_nodes({"n1", "n2"})

        mock_redis.xadd.assert_called_once()
        mock_redis.smembers.assert_not_called()

    @pytest.mark.asyncio
    async def test_stream_key_configured_at_init(self) -> None:
        mock_redis = AsyncMock()

        with patch(
            "orchestrator.app.semantic_cache"
            ".create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379",
                config=CacheConfig(similarity_threshold=0.9),
            )

        assert hasattr(cache, "_stream_key")
        assert "invalidation:stream" in cache._stream_key
        mock_redis.register_script.assert_not_called()


class TestRedisInvalidationStreamPublish:

    @pytest.mark.asyncio
    async def test_single_xadd_for_multiple_nodes(
        self,
    ) -> None:
        mock_redis = AsyncMock()

        with patch(
            "orchestrator.app.semantic_cache"
            ".create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379",
                config=CacheConfig(similarity_threshold=0.9),
            )
            cache._l1.store(
                "q1", [0.1] * 32, {"a": 1},
                tenant_id="t1", node_ids={"n1"},
            )
            cache._l1.store(
                "q2", [0.2] * 32, {"a": 2},
                tenant_id="t1", node_ids={"n2"},
            )
            cache._l1.store(
                "q3", [0.3] * 32, {"a": 3},
                tenant_id="t1", node_ids={"n1", "n2"},
            )
            await cache.invalidate_by_nodes({"n1", "n2"})

        assert mock_redis.xadd.call_count == 1
        mock_redis.smembers.assert_not_called()

    @pytest.mark.asyncio
    async def test_l1_invalidation_succeeds_on_stream_failure(
        self,
    ) -> None:
        mock_redis = AsyncMock()
        mock_redis.xadd = AsyncMock(
            side_effect=ConnectionError("redis down"),
        )

        with patch(
            "orchestrator.app.semantic_cache"
            ".create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379",
                config=CacheConfig(similarity_threshold=0.9),
            )
            cache._l1.store(
                "q1", [0.1] * 32, {"a": 1},
                tenant_id="t1", node_ids={"n1"},
            )
            removed = await cache.invalidate_by_nodes({"n1"})

        assert removed >= 1
        mock_redis.delete.assert_not_called()
