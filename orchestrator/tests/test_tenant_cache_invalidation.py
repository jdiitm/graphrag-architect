from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.subgraph_cache import SubgraphCache
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
    async def test_ingest_invalidation_rejects_empty_tenant(self) -> None:
        from orchestrator.app.graph_builder import (
            IngestRejectionError,
            invalidate_caches_after_ingest,
        )

        with pytest.raises(IngestRejectionError):
            await invalidate_caches_after_ingest(tenant_id="")

    @pytest.mark.asyncio
    async def test_sandboxed_read_cache_key_uses_tenant_prefix(self) -> None:
        from orchestrator.app.query_engine import _execute_sandboxed_read

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(return_value=[{"x": 1}])
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        test_cache = SubgraphCache(maxsize=256)
        cypher = "MATCH (n:Service) RETURN n"
        acl_params = {"tenant_id": "team-bravo", "is_admin": "false"}

        with (
            patch(
                "orchestrator.app.query_engine._SUBGRAPH_CACHE",
                new=test_cache,
            ),
            patch(
                "orchestrator.app.query_engine._get_query_timeout",
                return_value=30.0,
            ),
        ):
            await _execute_sandboxed_read(mock_driver, cypher, acl_params)

        matching_keys = [k for k in test_cache._cache if k.startswith("team-bravo:")]
        assert len(matching_keys) == 1

        removed = test_cache.invalidate_tenant("team-bravo")
        assert removed == 1
        assert test_cache.stats().size == 0
