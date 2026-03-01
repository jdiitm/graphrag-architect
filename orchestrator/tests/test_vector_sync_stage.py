from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.stages import IngestionStage
from orchestrator.app.stages.vector_sync_stage import VectorSyncStage


class TestVectorSyncStageProtocolConformance:

    def test_vector_sync_stage_implements_ingestion_stage_protocol(self) -> None:
        stage = VectorSyncStage(outbox=MagicMock())
        assert isinstance(stage, IngestionStage)


class TestVectorSyncStageExecute:

    @pytest.mark.asyncio
    async def test_execute_routes_events_to_outbox(self) -> None:
        mock_outbox = MagicMock()
        mock_outbox.enqueue = MagicMock()

        stage = VectorSyncStage(outbox=mock_outbox)
        state = {
            "mutation_events": [
                MagicMock(event_id="evt-1"),
                MagicMock(event_id="evt-2"),
            ],
        }
        result = await stage.execute(state)
        assert mock_outbox.enqueue.call_count == 2
        assert result.get("vector_sync_status") == "enqueued"

    @pytest.mark.asyncio
    async def test_execute_with_no_mutation_events_is_noop(self) -> None:
        mock_outbox = MagicMock()
        stage = VectorSyncStage(outbox=mock_outbox)
        state = {"mutation_events": []}
        result = await stage.execute(state)
        mock_outbox.enqueue.assert_not_called()
        assert result.get("vector_sync_status") == "skipped"

    @pytest.mark.asyncio
    async def test_execute_without_mutation_events_key_is_noop(self) -> None:
        mock_outbox = MagicMock()
        stage = VectorSyncStage(outbox=mock_outbox)
        state = {"other_data": "value"}
        result = await stage.execute(state)
        mock_outbox.enqueue.assert_not_called()
        assert result.get("vector_sync_status") == "skipped"

    @pytest.mark.asyncio
    async def test_execute_routes_to_kafka_backend_when_configured(self) -> None:
        mock_publisher = AsyncMock()
        mock_publisher.publish = AsyncMock()

        stage = VectorSyncStage(
            outbox=MagicMock(),
            kafka_publisher=mock_publisher,
            backend="kafka",
        )
        state = {
            "mutation_events": [MagicMock(event_id="evt-k1")],
        }
        result = await stage.execute(state)
        mock_publisher.publish.assert_called_once()
        assert result.get("vector_sync_status") == "published"


class TestVectorSyncStageHealthcheck:

    @pytest.mark.asyncio
    async def test_healthcheck_returns_true_for_memory_backend(self) -> None:
        stage = VectorSyncStage(outbox=MagicMock())
        assert await stage.healthcheck() is True

    @pytest.mark.asyncio
    async def test_healthcheck_returns_true_when_kafka_publisher_healthy(self) -> None:
        mock_publisher = AsyncMock()
        mock_publisher.healthcheck = AsyncMock(return_value=True)
        stage = VectorSyncStage(
            outbox=MagicMock(),
            kafka_publisher=mock_publisher,
            backend="kafka",
        )
        assert await stage.healthcheck() is True

    @pytest.mark.asyncio
    async def test_healthcheck_returns_false_when_kafka_publisher_down(self) -> None:
        mock_publisher = AsyncMock()
        mock_publisher.healthcheck = AsyncMock(return_value=False)
        stage = VectorSyncStage(
            outbox=MagicMock(),
            kafka_publisher=mock_publisher,
            backend="kafka",
        )
        assert await stage.healthcheck() is False
