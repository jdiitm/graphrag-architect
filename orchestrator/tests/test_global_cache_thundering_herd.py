from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestGlobalCacheInvalidationNoThunderingHerd:

    @pytest.mark.asyncio
    async def test_empty_tenant_must_not_call_invalidate_stale(self) -> None:
        mock_subgraph_cache = MagicMock()
        mock_subgraph_cache.advance_generation = MagicMock()
        mock_subgraph_cache.invalidate_stale = MagicMock()

        mock_semantic_cache = MagicMock()
        mock_semantic_cache.advance_generation = MagicMock()
        mock_semantic_cache.invalidate_stale = MagicMock()

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
            await invalidate_caches_after_ingest(tenant_id="")

        mock_subgraph_cache.advance_generation.assert_called_once()
        mock_subgraph_cache.invalidate_stale.assert_not_called()

        mock_semantic_cache.advance_generation.assert_called_once()
        mock_semantic_cache.invalidate_stale.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_tenant_logs_warning(self) -> None:
        mock_subgraph_cache = MagicMock()
        mock_subgraph_cache.advance_generation = MagicMock()

        mock_semantic_cache = None

        with (
            patch(
                "orchestrator.app.query_engine._SUBGRAPH_CACHE",
                mock_subgraph_cache,
            ),
            patch(
                "orchestrator.app.query_engine._SEMANTIC_CACHE",
                mock_semantic_cache,
            ),
            patch("orchestrator.app.graph_builder.logger") as mock_logger,
        ):
            from orchestrator.app.graph_builder import invalidate_caches_after_ingest
            await invalidate_caches_after_ingest(tenant_id="")

        warning_calls = [
            call for call in mock_logger.warning.call_args_list
            if "tenant_id" in str(call).lower()
        ]
        assert len(warning_calls) >= 1, (
            "Expected a warning log when empty tenant_id triggers global invalidation"
        )

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
