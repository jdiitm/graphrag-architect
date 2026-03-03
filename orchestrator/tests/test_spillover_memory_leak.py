from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.graph_builder import (
    _spillover_to_vector_outbox,
    create_durable_spillover_fn,
)
from orchestrator.app.vector_sync_outbox import VectorSyncEvent


class TestSpilloverMemoryLeak:

    @pytest.mark.asyncio
    async def test_durable_spillover_pending_cleared_after_dispatch(self) -> None:
        mock_store = AsyncMock()
        spillover = create_durable_spillover_fn(mock_store)
        event = VectorSyncEvent(
            collection="test",
            operation="upsert",
            pruned_ids=[],
            vectors=[],
        )
        spillover([event])
        assert len(spillover.pending) == 0

    def test_spillover_drift_metric_incremented(self) -> None:
        mock_metrics = MagicMock()
        with patch(
            "orchestrator.app.graph_builder.get_metrics_port",
            return_value=mock_metrics,
        ):
            event = VectorSyncEvent(
                collection="test",
                operation="upsert",
                pruned_ids=[],
                vectors=[],
            )
            _spillover_to_vector_outbox([event])
            mock_metrics.increment_counter.assert_called_once_with(
                "vector_sync.drift_risk_total", 1, {},
            )
