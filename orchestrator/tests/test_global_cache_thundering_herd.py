from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from orchestrator.app.graph_builder import IngestRejectionError


class TestGlobalCacheInvalidationNoThunderingHerd:

    @pytest.mark.asyncio
    async def test_empty_tenant_raises_rejection_error(self) -> None:
        from orchestrator.app.graph_builder import invalidate_caches_after_ingest

        with pytest.raises(IngestRejectionError):
            await invalidate_caches_after_ingest(tenant_id="")

    @pytest.mark.asyncio
    async def test_empty_tenant_never_calls_advance_generation(self) -> None:
        mock_subgraph_cache = MagicMock()
        mock_subgraph_cache.advance_generation = MagicMock()

        with (
            patch(
                "orchestrator.app.query_engine._SUBGRAPH_CACHE",
                mock_subgraph_cache,
            ),
        ):
            from orchestrator.app.graph_builder import invalidate_caches_after_ingest
            with pytest.raises(IngestRejectionError):
                await invalidate_caches_after_ingest(tenant_id="")

        mock_subgraph_cache.advance_generation.assert_not_called()

    @pytest.mark.asyncio
    async def test_scoped_tenant_still_calls_invalidate_tenant(self) -> None:
        mock_subgraph_cache = MagicMock()
        mock_subgraph_cache.invalidate_tenant = MagicMock()
        mock_subgraph_cache.advance_generation = MagicMock()

        mock_semantic_cache = MagicMock()
        mock_semantic_cache.invalidate_tenant = MagicMock()

        with (
            patch(
                "orchestrator.app.query_engine._SUBGRAPH_CACHE",
                mock_subgraph_cache,
            ),
            patch(
                "orchestrator.app.query_engine._SEMANTIC_CACHE",
                mock_semantic_cache,
            ),
        ):
            from orchestrator.app.graph_builder import invalidate_caches_after_ingest
            await invalidate_caches_after_ingest(tenant_id="team-alpha")

        mock_subgraph_cache.invalidate_tenant.assert_called_once_with("team-alpha")
        mock_semantic_cache.invalidate_tenant.assert_called_once_with("team-alpha")
        mock_subgraph_cache.advance_generation.assert_not_called()
