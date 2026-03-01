from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from orchestrator.app.config import ASTPoolConfig


class TestASTPoolConfigDefaults:

    def test_default_ceiling_is_8(self) -> None:
        config = ASTPoolConfig()
        assert config.ceiling == 8

    def test_frozen(self) -> None:
        config = ASTPoolConfig()
        with pytest.raises(AttributeError):
            config.ceiling = 16  # type: ignore[misc]


class TestASTPoolConfigFromEnv:

    def test_reads_env_var(self) -> None:
        with patch.dict(os.environ, {"AST_POOL_CEILING": "4"}, clear=False):
            config = ASTPoolConfig.from_env()
        assert config.ceiling == 4

    def test_missing_env_returns_default(self) -> None:
        env = {k: v for k, v in os.environ.items() if k != "AST_POOL_CEILING"}
        with patch.dict(os.environ, env, clear=True):
            config = ASTPoolConfig.from_env()
        assert config.ceiling == 8

    def test_clamped_to_min_one(self) -> None:
        with patch.dict(os.environ, {"AST_POOL_CEILING": "0"}, clear=False):
            config = ASTPoolConfig.from_env()
        assert config.ceiling >= 1

    def test_clamped_to_max_cpu_times_two(self) -> None:
        cpu_count = os.cpu_count() or 4
        absurdly_high = cpu_count * 10
        with patch.dict(
            os.environ, {"AST_POOL_CEILING": str(absurdly_high)}, clear=False,
        ):
            config = ASTPoolConfig.from_env()
        assert config.ceiling <= cpu_count * 2

    def test_negative_value_clamped_to_one(self) -> None:
        with patch.dict(os.environ, {"AST_POOL_CEILING": "-5"}, clear=False):
            config = ASTPoolConfig.from_env()
        assert config.ceiling == 1


class TestGraphBuilderUsesASTPoolConfig:

    def test_pool_ceiling_reads_from_config(self) -> None:
        with patch.dict(os.environ, {"AST_POOL_CEILING": "6"}, clear=False):
            from orchestrator.app.config import ASTPoolConfig
            config = ASTPoolConfig.from_env()
            assert config.ceiling == 6
