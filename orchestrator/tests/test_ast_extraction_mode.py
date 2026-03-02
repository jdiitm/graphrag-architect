from __future__ import annotations

import concurrent.futures
import os
from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock, patch

import pytest

_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}

_GO_HTTP_SERVER_SRC = (
    "package main\n"
    'import "net/http"\n'
    "func main() {\n"
    '  http.ListenAndServe(":8080", nil)\n'
    "}\n"
)


class TestASTExtractionConfigDefaults:

    def test_defaults_to_go_mode_in_production(self) -> None:
        from orchestrator.app.config import ASTExtractionConfig

        with patch.dict("os.environ", {"DEPLOYMENT_MODE": "production"}, clear=False):
            os.environ.pop("AST_EXTRACTION_MODE", None)
            cfg = ASTExtractionConfig.from_env()

        assert cfg.mode == "go"

    def test_defaults_to_local_mode_in_dev(self) -> None:
        from orchestrator.app.config import ASTExtractionConfig

        with patch.dict("os.environ", {"DEPLOYMENT_MODE": "dev"}, clear=False):
            os.environ.pop("AST_EXTRACTION_MODE", None)
            cfg = ASTExtractionConfig.from_env()

        assert cfg.mode == "local"

    def test_env_var_overrides_deployment_mode(self) -> None:
        from orchestrator.app.config import ASTExtractionConfig

        with patch.dict("os.environ", {
            "DEPLOYMENT_MODE": "dev",
            "AST_EXTRACTION_MODE": "go",
        }):
            cfg = ASTExtractionConfig.from_env()

        assert cfg.mode == "go"


class TestASTExtractionConfigValidation:

    def test_frozen_dataclass_prevents_mutation(self) -> None:
        from orchestrator.app.config import ASTExtractionConfig

        cfg = ASTExtractionConfig(mode="go")
        with pytest.raises(FrozenInstanceError):
            cfg.mode = "local"

    def test_invalid_mode_raises_value_error(self) -> None:
        from orchestrator.app.config import ASTExtractionConfig

        with patch.dict("os.environ", {"AST_EXTRACTION_MODE": "invalid"}):
            with pytest.raises(ValueError, match="AST_EXTRACTION_MODE"):
                ASTExtractionConfig.from_env()

    def test_local_mode_accepted(self) -> None:
        from orchestrator.app.config import ASTExtractionConfig

        with patch.dict("os.environ", {"AST_EXTRACTION_MODE": "local"}):
            cfg = ASTExtractionConfig.from_env()

        assert cfg.mode == "local"


class TestGoModeSkipsProcessPool:

    @pytest.mark.asyncio
    async def test_go_mode_does_not_invoke_process_pool(self) -> None:
        from orchestrator.app.graph_builder import parse_source_ast

        state = {
            "raw_files": [
                {"path": "auth/main.go", "content": _GO_HTTP_SERVER_SRC},
            ],
            "extracted_nodes": [],
            "extraction_checkpoint": {},
            "tenant_id": "test-tenant",
        }

        with (
            patch.dict("os.environ", {**_ENV_VARS, "AST_EXTRACTION_MODE": "go"}),
            patch(
                "orchestrator.app.graph_builder._get_process_pool",
                side_effect=AssertionError(
                    "ProcessPoolExecutor must not be used in go mode"
                ),
            ),
            patch(
                "orchestrator.app.graph_builder._USE_REMOTE_AST",
                False,
            ),
        ):
            result = await parse_source_ast(state)

        assert result["extracted_nodes"] == []

    @pytest.mark.asyncio
    async def test_go_mode_passthrough_returns_empty_nodes(self) -> None:
        from orchestrator.app.graph_builder import parse_source_ast

        state = {
            "raw_files": [
                {"path": "auth/main.go", "content": _GO_HTTP_SERVER_SRC},
                {
                    "path": "lib/app.py",
                    "content": "from fastapi import FastAPI\napp = FastAPI()",
                },
            ],
            "extracted_nodes": [],
            "extraction_checkpoint": {},
            "tenant_id": "test-tenant",
        }

        async def _fake_run_in_executor(pool, fn, *args):
            return fn(*args)

        mock_loop = MagicMock()
        mock_loop.run_in_executor = _fake_run_in_executor

        with (
            patch.dict("os.environ", {**_ENV_VARS, "AST_EXTRACTION_MODE": "go"}),
            patch(
                "orchestrator.app.graph_builder._USE_REMOTE_AST",
                False,
            ),
            patch(
                "orchestrator.app.graph_builder._get_process_pool",
                return_value=MagicMock(),
            ),
            patch(
                "asyncio.get_running_loop",
                return_value=mock_loop,
            ),
        ):
            result = await parse_source_ast(state)

        assert result["extracted_nodes"] == []
        assert "extraction_checkpoint" in result


class TestLocalModePreservesProcessPool:

    @pytest.mark.asyncio
    async def test_local_mode_uses_process_pool(self) -> None:
        from orchestrator.app.graph_builder import parse_source_ast

        state = {
            "raw_files": [
                {"path": "auth/main.go", "content": _GO_HTTP_SERVER_SRC},
            ],
            "extracted_nodes": [],
            "extraction_checkpoint": {},
            "tenant_id": "test-tenant",
        }

        pool_was_used = []

        def _tracking_pool():
            pool_was_used.append(True)
            return MagicMock()

        async def _fake_run_in_executor(pool, fn, *args):
            return fn(*args)

        mock_loop = MagicMock()
        mock_loop.run_in_executor = _fake_run_in_executor

        with (
            patch.dict("os.environ", {**_ENV_VARS, "AST_EXTRACTION_MODE": "local"}),
            patch(
                "orchestrator.app.graph_builder._USE_REMOTE_AST",
                False,
            ),
            patch(
                "orchestrator.app.graph_builder._get_process_pool",
                _tracking_pool,
            ),
            patch(
                "asyncio.get_running_loop",
                return_value=mock_loop,
            ),
        ):
            result = await parse_source_ast(state)

        assert pool_was_used, (
            "When AST_EXTRACTION_MODE=local, ProcessPoolExecutor should be used"
        )
        assert "extracted_nodes" in result


class TestShutdownProcessPool:

    def test_shutdown_cleans_up_pool(self) -> None:
        from orchestrator.app.graph_builder import (
            _ASTPoolHolder,
            _shutdown_process_pool,
        )

        mock_pool = MagicMock(spec=concurrent.futures.ProcessPoolExecutor)
        original = _ASTPoolHolder.instance
        _ASTPoolHolder.instance = mock_pool

        try:
            _shutdown_process_pool()
            mock_pool.shutdown.assert_called_once_with(wait=False)
            assert _ASTPoolHolder.instance is None
        finally:
            _ASTPoolHolder.instance = original

    def test_shutdown_noop_when_no_pool(self) -> None:
        from orchestrator.app.graph_builder import (
            _ASTPoolHolder,
            _shutdown_process_pool,
        )

        original = _ASTPoolHolder.instance
        _ASTPoolHolder.instance = None

        try:
            _shutdown_process_pool()
            assert _ASTPoolHolder.instance is None
        finally:
            _ASTPoolHolder.instance = original
