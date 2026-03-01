from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.stages import IngestionStage
from orchestrator.app.stages.extraction_stage import ExtractionStage


class TestExtractionStageProtocolConformance:

    def test_extraction_stage_implements_ingestion_stage_protocol(self) -> None:
        stage = ExtractionStage(extractor=MagicMock())
        assert isinstance(stage, IngestionStage)


class TestExtractionStageExecute:

    @pytest.mark.asyncio
    async def test_execute_invokes_llm_extraction_on_ast_data(self) -> None:
        mock_extractor = AsyncMock()
        mock_extractor.extract_all = AsyncMock(return_value=MagicMock(
            services=[MagicMock(id="svc-1", confidence=0.7)],
            calls=[],
        ))

        stage = ExtractionStage(extractor=mock_extractor)
        state = {
            "raw_files": [
                {"path": "main.go", "content": "package main"},
            ],
            "ast_results": [MagicMock()],
            "extracted_nodes": [],
        }
        result = await stage.execute(state)
        mock_extractor.extract_all.assert_called_once()
        assert "extracted_nodes" in result

    @pytest.mark.asyncio
    async def test_execute_with_no_raw_files_is_noop(self) -> None:
        mock_extractor = AsyncMock()
        stage = ExtractionStage(extractor=mock_extractor)
        state = {"raw_files": [], "extracted_nodes": ["existing"]}
        result = await stage.execute(state)
        mock_extractor.extract_all.assert_not_called()
        assert result["extracted_nodes"] == ["existing"]

    @pytest.mark.asyncio
    async def test_execute_preserves_existing_nodes(self) -> None:
        mock_extractor = AsyncMock()
        mock_extractor.extract_all = AsyncMock(return_value=MagicMock(
            services=[], calls=[],
        ))
        stage = ExtractionStage(extractor=mock_extractor)
        state = {
            "raw_files": [{"path": "a.go", "content": "package a"}],
            "extracted_nodes": ["pre-existing-node"],
        }
        result = await stage.execute(state)
        assert "pre-existing-node" in result["extracted_nodes"]


class TestExtractionStageHealthcheck:

    @pytest.mark.asyncio
    async def test_healthcheck_returns_true(self) -> None:
        stage = ExtractionStage(extractor=MagicMock())
        assert await stage.healthcheck() is True
