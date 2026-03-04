from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Set
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.semantic_cache import CacheConfig, SemanticQueryCache
from orchestrator.app.subgraph_cache import SubgraphCache


class TestSubgraphCacheSWR:

    def test_get_or_stale_returns_fresh_hit(self) -> None:
        cache = SubgraphCache(maxsize=64)
        cache.put("key1", [{"name": "svc-a"}])
        result = cache.get_or_stale("key1")
        assert result is not None
        value, is_stale = result
        assert value == [{"name": "svc-a"}]
        assert is_stale is False

    def test_get_or_stale_returns_stale_after_generation_advance(self) -> None:
        cache = SubgraphCache(maxsize=64)
        cache.put("key1", [{"name": "svc-a"}])
        cache.advance_generation()
        result = cache.get_or_stale("key1")
        assert result is not None
        value, is_stale = result
        assert value == [{"name": "svc-a"}]
        assert is_stale is True

    def test_get_or_stale_returns_none_for_missing_key(self) -> None:
        cache = SubgraphCache(maxsize=64)
        result = cache.get_or_stale("nonexistent")
        assert result is None

    def test_advance_generation_preserves_entries(self) -> None:
        cache = SubgraphCache(maxsize=64)
        cache.put("key1", [{"name": "svc-a"}])
        cache.put("key2", [{"name": "svc-b"}])
        initial_size = cache.stats().size
        cache.advance_generation()
        assert cache.stats().size == initial_size
        assert cache.get("key1") == [{"name": "svc-a"}]
        assert cache.get("key2") == [{"name": "svc-b"}]

    def test_invalidate_tenant_drops_entries_but_advance_does_not(self) -> None:
        cache = SubgraphCache(maxsize=64)
        cache.put("tenant-a:q1", [{"name": "svc-a"}])
        cache.put("tenant-a:q2", [{"name": "svc-b"}])
        cache.advance_generation()
        assert cache.stats().size == 2
        cache.invalidate_stale()
        assert cache.stats().size == 0

    def test_swr_concurrent_reads_all_get_stale(self) -> None:
        cache = SubgraphCache(maxsize=64)
        cache.put("key1", [{"name": "svc-a"}])
        cache.advance_generation()
        results = [cache.get_or_stale("key1") for _ in range(100)]
        assert all(r is not None for r in results)
        assert all(r[1] is True for r in results)  # type: ignore[index]
        assert all(r[0] == [{"name": "svc-a"}] for r in results)  # type: ignore[index]

    def test_put_after_advance_makes_entry_fresh(self) -> None:
        cache = SubgraphCache(maxsize=64)
        cache.put("key1", [{"name": "old"}])
        cache.advance_generation()
        result = cache.get_or_stale("key1")
        assert result is not None and result[1] is True
        cache.put("key1", [{"name": "new"}])
        result = cache.get_or_stale("key1")
        assert result is not None
        assert result[0] == [{"name": "new"}]
        assert result[1] is False


class TestSemanticCacheSWR:

    def test_lookup_swr_returns_fresh_result(self) -> None:
        config = CacheConfig(similarity_threshold=0.9)
        cache = SemanticQueryCache(config=config)
        embedding = [1.0, 0.0, 0.0]
        cache.store(
            query="test query",
            query_embedding=embedding,
            result={"answer": "ok"},
            tenant_id="t1",
            acl_key="team:a",
        )
        result = cache.lookup_swr(
            query_embedding=embedding,
            tenant_id="t1",
            acl_key="team:a",
        )
        assert result is not None
        value, is_stale = result
        assert value == {"answer": "ok"}
        assert is_stale is False

    def test_lookup_swr_returns_stale_after_generation_advance(self) -> None:
        config = CacheConfig(similarity_threshold=0.9)
        cache = SemanticQueryCache(config=config)
        embedding = [1.0, 0.0, 0.0]
        cache.store(
            query="test query",
            query_embedding=embedding,
            result={"answer": "ok"},
            tenant_id="t1",
            acl_key="team:a",
        )
        cache.advance_generation()
        result = cache.lookup_swr(
            query_embedding=embedding,
            tenant_id="t1",
            acl_key="team:a",
        )
        assert result is not None
        value, is_stale = result
        assert value == {"answer": "ok"}
        assert is_stale is True

    def test_lookup_swr_returns_none_for_no_match(self) -> None:
        config = CacheConfig(similarity_threshold=0.99)
        cache = SemanticQueryCache(config=config)
        result = cache.lookup_swr(
            query_embedding=[1.0, 0.0, 0.0],
            tenant_id="t1",
            acl_key="team:a",
        )
        assert result is None


class TestGraphBuilderSWRIntegration:

    @pytest.mark.asyncio
    async def test_invalidate_uses_generation_advance_on_fallback(self) -> None:
        mock_subgraph = MagicMock()
        mock_subgraph.advance_generation = MagicMock(return_value=2)
        mock_subgraph.invalidate_stale = MagicMock(return_value=0)

        mock_semantic = MagicMock()
        mock_semantic.advance_generation = MagicMock(return_value=2)
        mock_semantic.invalidate_stale = MagicMock(return_value=0)
        mock_semantic.invalidate_by_nodes = MagicMock()

        with patch(
            "orchestrator.app.query_engine._SUBGRAPH_CACHE", mock_subgraph,
        ), patch(
            "orchestrator.app.query_engine._SEMANTIC_CACHE", mock_semantic,
        ):
            from orchestrator.app.graph_builder import invalidate_caches_after_ingest
            await invalidate_caches_after_ingest(
                tenant_id="tenant-a", node_ids=None,
            )

        mock_subgraph.advance_generation.assert_called_once()
        mock_subgraph.invalidate_tenant.assert_not_called()
        mock_semantic.advance_generation.assert_called_once()
        mock_semantic.invalidate_tenant.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalidate_still_uses_targeted_when_node_ids_present(self) -> None:
        mock_subgraph = MagicMock()
        mock_subgraph.invalidate_by_nodes = MagicMock(return_value=1)

        mock_semantic = MagicMock()
        mock_semantic.invalidate_by_nodes = MagicMock(return_value=1)

        with patch(
            "orchestrator.app.query_engine._SUBGRAPH_CACHE", mock_subgraph,
        ), patch(
            "orchestrator.app.query_engine._SEMANTIC_CACHE", mock_semantic,
        ):
            from orchestrator.app.graph_builder import invalidate_caches_after_ingest
            await invalidate_caches_after_ingest(
                tenant_id="tenant-a", node_ids={"node-1", "node-2"},
            )

        mock_subgraph.invalidate_by_nodes.assert_called_once_with(
            {"node-1", "node-2"},
        )
