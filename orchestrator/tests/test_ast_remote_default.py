from __future__ import annotations

import logging
import os
from unittest.mock import patch

import pytest

from orchestrator.app.config import ASTExtractionConfig, ConfigurationError


class TestASTRemoteDefault:

    def test_production_defaults_to_go_mode(self) -> None:
        with patch.dict(
            os.environ,
            {"DEPLOYMENT_MODE": "production", "AST_EXTRACTION_MODE": ""},
            clear=False,
        ):
            env = dict(os.environ)
            env.pop("AST_EXTRACTION_MODE", None)
            with patch.dict(os.environ, env, clear=True):
                cfg = ASTExtractionConfig.from_env()
                assert cfg.mode == "go"

    def test_dev_defaults_to_local_mode(self) -> None:
        env = {k: v for k, v in os.environ.items() if k != "AST_EXTRACTION_MODE"}
        env["DEPLOYMENT_MODE"] = "dev"
        with patch.dict(os.environ, env, clear=True):
            cfg = ASTExtractionConfig.from_env()
            assert cfg.mode == "local"

    def test_explicit_mode_overrides_default(self) -> None:
        with patch.dict(
            os.environ,
            {"AST_EXTRACTION_MODE": "go", "DEPLOYMENT_MODE": "dev"},
            clear=False,
        ):
            cfg = ASTExtractionConfig.from_env()
            assert cfg.mode == "go"

    def test_invalid_mode_raises(self) -> None:
        with patch.dict(os.environ, {"AST_EXTRACTION_MODE": "invalid"}):
            with pytest.raises(ValueError, match="AST_EXTRACTION_MODE"):
                ASTExtractionConfig.from_env()


class TestUseRemoteASTAlignment:

    def test_use_remote_ast_true_when_go_mode(self) -> None:
        from orchestrator.app.graph_builder import resolve_use_remote_ast
        assert resolve_use_remote_ast("go") is True

    def test_use_remote_ast_false_when_local_mode(self) -> None:
        from orchestrator.app.graph_builder import resolve_use_remote_ast
        assert resolve_use_remote_ast("local") is False


class TestProductionLocalModeWarning:

    def test_production_local_mode_raises_configuration_error(self) -> None:
        with patch.dict(
            os.environ,
            {"DEPLOYMENT_MODE": "production", "AST_EXTRACTION_MODE": "local"},
            clear=False,
        ):
            from orchestrator.app.graph_builder import (
                warn_if_local_ast_in_production,
            )
            with pytest.raises(ConfigurationError, match="CPU starvation"):
                warn_if_local_ast_in_production()

    def test_dev_local_mode_no_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        with patch.dict(
            os.environ,
            {"DEPLOYMENT_MODE": "dev", "AST_EXTRACTION_MODE": "local"},
            clear=False,
        ):
            with caplog.at_level(logging.WARNING):
                from orchestrator.app.graph_builder import (
                    warn_if_local_ast_in_production,
                )
                warn_if_local_ast_in_production()
            ast_warnings = [
                r for r in caplog.records
                if "local" in r.message.lower() and "production" in r.message.lower()
            ]
            assert len(ast_warnings) == 0
