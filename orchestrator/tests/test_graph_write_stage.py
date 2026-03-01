from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.stages import IngestionStage
from orchestrator.app.stages.graph_write_stage import GraphWriteStage


class TestGraphWriteStageProtocolConformance:

    def test_graph_write_stage_implements_ingestion_stage_protocol(self) -> None:
        stage = GraphWriteStage(repository=MagicMock())
        assert isinstance(stage, IngestionStage)


class TestGraphWriteStageExecute:

    @pytest.mark.asyncio
    async def test_execute_writes_entities_to_neo4j(self) -> None:
        mock_repo = AsyncMock()
        mock_repo.commit_topology = AsyncMock()

        stage = GraphWriteStage(repository=mock_repo)
        state = {
            "extracted_nodes": [MagicMock(), MagicMock()],
        }
        result = await stage.execute(state)
        mock_repo.commit_topology.assert_called_once()
        assert result["commit_status"] == "success"

    @pytest.mark.asyncio
    async def test_execute_with_empty_extraction_data_is_noop(self) -> None:
        mock_repo = AsyncMock()
        stage = GraphWriteStage(repository=mock_repo)
        state = {"extracted_nodes": []}
        result = await stage.execute(state)
        mock_repo.commit_topology.assert_not_called()
        assert result["commit_status"] == "skipped"

    @pytest.mark.asyncio
    async def test_execute_records_failure_on_repository_error(self) -> None:
        mock_repo = AsyncMock()
        mock_repo.commit_topology = AsyncMock(
            side_effect=RuntimeError("Neo4j connection lost"),
        )

        stage = GraphWriteStage(repository=mock_repo)
        state = {"extracted_nodes": [MagicMock()]}
        result = await stage.execute(state)
        assert result["commit_status"] == "failed"


class TestGraphWriteStageHealthcheck:

    @pytest.mark.asyncio
    async def test_healthcheck_returns_true_when_driver_available(self) -> None:
        mock_repo = AsyncMock()
        mock_repo.read_topology = AsyncMock(return_value=[])
        stage = GraphWriteStage(repository=mock_repo)
        assert await stage.healthcheck() is True

    @pytest.mark.asyncio
    async def test_healthcheck_returns_false_when_driver_unreachable(self) -> None:
        mock_repo = AsyncMock()
        mock_repo.read_topology = AsyncMock(
            side_effect=RuntimeError("unreachable"),
        )
        stage = GraphWriteStage(repository=mock_repo)
        assert await stage.healthcheck() is False
