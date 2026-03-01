from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.stages import IngestionStage
from orchestrator.app.stages.ast_stage import ASTStage


class TestASTStageProtocolConformance:

    def test_ast_stage_implements_ingestion_stage_protocol(self) -> None:
        stage = ASTStage()
        assert isinstance(stage, IngestionStage)


class TestASTStageExecute:

    @pytest.mark.asyncio
    async def test_execute_with_pending_files_returns_ast_results(self) -> None:
        stage = ASTStage()
        state = {
            "pending_files": [
                {"path": "main.go", "content": "package main"},
                {"path": "app.py", "content": "from fastapi import FastAPI"},
            ],
        }
        result = await stage.execute(state)
        assert "ast_results" in result

    @pytest.mark.asyncio
    async def test_execute_with_empty_file_list_returns_unchanged_state(self) -> None:
        stage = ASTStage()
        state = {"pending_files": [], "existing_key": "preserved"}
        result = await stage.execute(state)
        assert result.get("existing_key") == "preserved"
        assert "ast_results" not in result or result["ast_results"] == []

    @pytest.mark.asyncio
    async def test_execute_without_pending_files_key_is_noop(self) -> None:
        stage = ASTStage()
        state = {"some_other_data": 42}
        result = await stage.execute(state)
        assert result.get("some_other_data") == 42


class TestASTStageHealthcheck:

    @pytest.mark.asyncio
    async def test_healthcheck_returns_true_when_pool_available(self) -> None:
        stage = ASTStage()
        assert await stage.healthcheck() is True


class TestASTStageGRPCRouting:

    @pytest.mark.asyncio
    async def test_routes_to_grpc_when_client_configured_and_available(self) -> None:
        mock_grpc = AsyncMock()
        mock_grpc.is_available = True
        mock_grpc.extract_batch = AsyncMock(return_value=[
            MagicMock(services=[], calls=[]),
        ])

        stage = ASTStage(grpc_client=mock_grpc)
        state = {
            "pending_files": [
                {"path": "svc/main.go", "content": "package main"},
            ],
            "tenant_id": "test-tenant",
        }
        result = await stage.execute(state)
        mock_grpc.extract_batch.assert_called_once()
        assert "ast_results" in result

    @pytest.mark.asyncio
    async def test_falls_back_to_local_when_grpc_unavailable(self) -> None:
        mock_grpc = MagicMock()
        mock_grpc.is_available = False

        stage = ASTStage(grpc_client=mock_grpc)
        state = {
            "pending_files": [
                {"path": "main.go", "content": "package main"},
            ],
        }
        result = await stage.execute(state)
        assert "ast_results" in result
