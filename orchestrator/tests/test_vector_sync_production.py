from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from orchestrator.app.config import ConfigurationError
from orchestrator.app.vector_sync_outbox import validate_production_sync_mode


class TestProductionSyncModeEnforcement:

    def test_production_mode_rejects_memory_sync(self) -> None:
        with patch.dict(os.environ, {"DEPLOYMENT_MODE": "production", "VECTOR_SYNC_MODE": "memory"}):
            with pytest.raises(ConfigurationError):
                validate_production_sync_mode()

    def test_production_mode_rejects_default_sync(self) -> None:
        with patch.dict(os.environ, {"DEPLOYMENT_MODE": "production"}):
            os.environ.pop("VECTOR_SYNC_MODE", None)
            with pytest.raises(ConfigurationError):
                validate_production_sync_mode()

    def test_production_mode_accepts_kafka_sync(self) -> None:
        with patch.dict(os.environ, {"DEPLOYMENT_MODE": "production", "VECTOR_SYNC_MODE": "kafka"}):
            validate_production_sync_mode()

    def test_production_mode_accepts_durable_sync(self) -> None:
        with patch.dict(os.environ, {"DEPLOYMENT_MODE": "production", "VECTOR_SYNC_MODE": "durable"}):
            validate_production_sync_mode()

    def test_dev_mode_allows_memory_sync(self) -> None:
        with patch.dict(os.environ, {"DEPLOYMENT_MODE": "dev", "VECTOR_SYNC_MODE": "memory"}):
            validate_production_sync_mode()
