from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.semantic_cache import (
    CacheConfig,
    RedisSemanticQueryCache,
    SemanticQueryCache,
)


class TestSemanticCacheNodeTagging:

    def test_store_with_node_ids_creates_tags(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        cache.store(
            "q", embedding, {"answer": "a"},
            tenant_id="t1", node_ids={"node-1", "node-2"},
        )
        assert cache.stats().size == 1

    def test_store_without_node_ids_still_works(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "a"}, tenant_id="t1")
        assert cache.stats().size == 1


class TestSemanticCacheInvalidateByNodes:

    def test_invalidate_removes_entries_tagged_with_node(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.store(
            "q1", [1.0, 0.0], {"a": 1},
            tenant_id="t1", node_ids={"node-A"},
        )
        cache.store(
            "q2", [0.0, 1.0], {"a": 2},
            tenant_id="t1", node_ids={"node-B"},
        )

        removed = cache.invalidate_by_nodes({"node-A"})

        assert removed == 1
        assert cache.stats().size == 1
        assert cache.lookup([0.0, 1.0], tenant_id="t1") == {"a": 2}

    def test_invalidate_multiple_nodes_removes_all_tagged(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.store(
            "q1", [1.0, 0.0], {"a": 1},
            tenant_id="t1", node_ids={"node-A"},
        )
        cache.store(
            "q2", [0.0, 1.0], {"a": 2},
            tenant_id="t1", node_ids={"node-B"},
        )
        cache.store(
            "q3", [0.5, 0.5], {"a": 3},
            tenant_id="t1", node_ids={"node-C"},
        )

        removed = cache.invalidate_by_nodes({"node-A", "node-B"})

        assert removed == 2
        assert cache.stats().size == 1

    def test_invalidate_shared_node_removes_all_entries(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.store(
            "q1", [1.0, 0.0], {"a": 1},
            tenant_id="t1", node_ids={"shared-node", "node-X"},
        )
        cache.store(
            "q2", [0.0, 1.0], {"a": 2},
            tenant_id="t1", node_ids={"shared-node"},
        )

        removed = cache.invalidate_by_nodes({"shared-node"})

        assert removed == 2
        assert cache.stats().size == 0

    def test_invalidate_nonexistent_node_returns_zero(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.store(
            "q1", [1.0, 0.0], {"a": 1},
            tenant_id="t1", node_ids={"node-A"},
        )

        removed = cache.invalidate_by_nodes({"nonexistent"})

        assert removed == 0
        assert cache.stats().size == 1

    def test_invalidate_empty_set_returns_zero(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.store(
            "q1", [1.0, 0.0], {"a": 1},
            tenant_id="t1", node_ids={"node-A"},
        )

        removed = cache.invalidate_by_nodes(set())

        assert removed == 0
        assert cache.stats().size == 1

    def test_invalidate_entry_without_tags_not_removed(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.store("q1", [1.0, 0.0], {"a": 1}, tenant_id="t1")
        cache.store(
            "q2", [0.0, 1.0], {"a": 2},
            tenant_id="t1", node_ids={"node-A"},
        )

        removed = cache.invalidate_by_nodes({"node-A"})

        assert removed == 1
        assert cache.stats().size == 1


class TestRedisSemanticCacheInvalidateByNodes:

    @pytest.mark.asyncio
    async def test_invalidate_by_nodes_delegates_to_l1(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.smembers = AsyncMock(return_value=set())
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
                "q1", [1.0, 0.0], {"a": 1},
                tenant_id="t1", node_ids={"node-A"},
            )
            cache._l1.store(
                "q2", [0.0, 1.0], {"a": 2},
                tenant_id="t1", node_ids={"node-B"},
            )

            removed = await cache.invalidate_by_nodes({"node-A"})

            assert removed == 1
            assert cache._l1.stats().size == 1


class TestGraphBuilderSemanticCacheNodeInvalidation:

    @pytest.mark.asyncio
    async def test_invalidate_caches_calls_semantic_invalidate_by_nodes(
        self,
    ) -> None:
        from orchestrator.app.graph_builder import invalidate_caches_after_ingest

        mock_subgraph = MagicMock()
        mock_subgraph.invalidate_by_nodes = MagicMock(return_value=2)
        mock_semantic = MagicMock()
        mock_semantic.invalidate_by_nodes = MagicMock(return_value=1)

        with patch(
            "orchestrator.app.query_engine._SUBGRAPH_CACHE", mock_subgraph,
        ), patch(
            "orchestrator.app.query_engine._SEMANTIC_CACHE", mock_semantic,
        ):
            await invalidate_caches_after_ingest(
                tenant_id="t1", node_ids={"node-1", "node-2"},
            )

        mock_subgraph.invalidate_by_nodes.assert_called_once_with(
            {"node-1", "node-2"},
        )
        mock_semantic.invalidate_by_nodes.assert_called_once_with(
            {"node-1", "node-2"},
        )

    @pytest.mark.asyncio
    async def test_invalidate_falls_back_to_tenant_when_no_node_ids(
        self,
    ) -> None:
        from orchestrator.app.graph_builder import invalidate_caches_after_ingest

        mock_subgraph = MagicMock()
        mock_subgraph.invalidate_tenant = MagicMock(return_value=2)
        mock_semantic = MagicMock()
        mock_semantic.invalidate_tenant = MagicMock(return_value=1)

        with patch(
            "orchestrator.app.query_engine._SUBGRAPH_CACHE", mock_subgraph,
        ), patch(
            "orchestrator.app.query_engine._SEMANTIC_CACHE", mock_semantic,
        ):
            await invalidate_caches_after_ingest(
                tenant_id="t1", node_ids=None,
            )

        mock_subgraph.invalidate_tenant.assert_called_once_with("t1")
        mock_semantic.invalidate_tenant.assert_called_once_with("t1")
