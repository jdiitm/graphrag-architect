from __future__ import annotations

from unittest.mock import patch

import pytest

from orchestrator.app.config import VectorStoreConfig


class TestVectorStoreConfigDeploymentMode:

    def test_config_has_deployment_mode_field(self) -> None:
        cfg = VectorStoreConfig(deployment_mode="production")
        assert cfg.deployment_mode == "production"

    def test_config_defaults_to_dev(self) -> None:
        cfg = VectorStoreConfig()
        assert cfg.deployment_mode == "dev"

    def test_from_env_reads_deployment_mode(self) -> None:
        with patch.dict(
            "os.environ",
            {"DEPLOYMENT_MODE": "production"},
            clear=True,
        ):
            cfg = VectorStoreConfig.from_env()
            assert cfg.deployment_mode == "production"

    def test_from_env_defaults_deployment_mode_to_dev(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            cfg = VectorStoreConfig.from_env()
            assert cfg.deployment_mode == "dev"
