from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.subgraph_cache import SubgraphCache, cache_key
from orchestrator.app.semantic_cache import SemanticQueryCache


class TestSubgraphCacheTenantInvalidation:
    def test_invalidate_tenant_removes_matching_keys(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("tenant_a:query1", [{"name": "svc-a"}])
        cache.put("tenant_a:query2", [{"name": "svc-b"}])
        cache.put("tenant_b:query1", [{"name": "svc-c"}])

        removed = cache.invalidate_tenant("tenant_a")

        assert removed == 2
        assert cache.get("tenant_a:query1") is None
        assert cache.get("tenant_a:query2") is None
        assert cache.get("tenant_b:query1") is not None

    def test_invalidate_tenant_empty_id_removes_nothing(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("tenant_a:query1", [{"name": "svc-a"}])

        removed = cache.invalidate_tenant("")

        assert removed == 0
        assert cache.get("tenant_a:query1") is not None

    def test_invalidate_tenant_nonexistent_removes_nothing(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("tenant_a:query1", [{"name": "svc-a"}])

        removed = cache.invalidate_tenant("tenant_z")

        assert removed == 0

    def test_invalidate_tenant_preserves_other_tenants(self) -> None:
        cache = SubgraphCache(maxsize=256)
        for t in ["alpha", "beta", "gamma"]:
            cache.put(f"{t}:q1", [{"tenant": t}])

        cache.invalidate_tenant("beta")

        assert cache.get("alpha:q1") is not None
        assert cache.get("beta:q1") is None
        assert cache.get("gamma:q1") is not None


class TestSemanticCacheTenantInvalidation:
    def test_invalidate_tenant_removes_matching_entries(self) -> None:
        cache = SemanticQueryCache()
        emb_a = [1.0, 0.0, 0.0]
        emb_b = [0.0, 1.0, 0.0]

        cache.store("q1", emb_a, {"answer": "a"}, tenant_id="tenant_a")
        cache.store("q2", emb_b, {"answer": "b"}, tenant_id="tenant_b")

        removed = cache.invalidate_tenant("tenant_a")

        assert removed == 1
        assert cache.lookup(emb_a, tenant_id="tenant_a") is None
        assert cache.lookup(emb_b, tenant_id="tenant_b") is not None

    def test_invalidate_tenant_empty_id_removes_nothing(self) -> None:
        cache = SemanticQueryCache()
        emb = [1.0, 0.0, 0.0]
        cache.store("q1", emb, {"answer": "a"}, tenant_id="tenant_a")

        removed = cache.invalidate_tenant("")

        assert removed == 0


_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


class TestGranularCacheInvalidation:
    @pytest.mark.asyncio
    async def test_ingest_invalidation_passes_tenant_id(self) -> None:
        from orchestrator.app.graph_builder import invalidate_caches_after_ingest

        subgraph_cache = MagicMock()
        subgraph_cache.invalidate_tenant = MagicMock(return_value=2)

        semantic_cache = MagicMock()
        semantic_cache.invalidate_tenant = MagicMock(return_value=1)

        with (
            patch(
                "orchestrator.app.query_engine._SUBGRAPH_CACHE",
                new=subgraph_cache,
            ),
            patch(
                "orchestrator.app.query_engine._SEMANTIC_CACHE",
                new=semantic_cache,
            ),
        ):
            await invalidate_caches_after_ingest(tenant_id="team-alpha")

        subgraph_cache.invalidate_tenant.assert_called_once_with("team-alpha")
        semantic_cache.invalidate_tenant.assert_called_once_with("team-alpha")

    @pytest.mark.asyncio
    async def test_ingest_invalidation_global_when_no_tenant(self) -> None:
        from orchestrator.app.graph_builder import invalidate_caches_after_ingest

        subgraph_cache = MagicMock()
        subgraph_cache.advance_generation = MagicMock(return_value=1)
        subgraph_cache.invalidate_stale = MagicMock(return_value=0)

        semantic_cache = MagicMock()
        semantic_cache.advance_generation = MagicMock(return_value=1)
        semantic_cache.invalidate_stale = AsyncMock(return_value=0)

        with (
            patch(
                "orchestrator.app.query_engine._SUBGRAPH_CACHE",
                new=subgraph_cache,
            ),
            patch(
                "orchestrator.app.query_engine._SEMANTIC_CACHE",
                new=semantic_cache,
            ),
        ):
            await invalidate_caches_after_ingest(tenant_id="")

        subgraph_cache.advance_generation.assert_called_once()
        subgraph_cache.invalidate_stale.assert_called_once()
        semantic_cache.advance_generation.assert_called_once()
