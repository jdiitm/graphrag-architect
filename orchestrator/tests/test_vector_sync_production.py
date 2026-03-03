from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

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


_LIFESPAN_PATCHES = {
    "orchestrator.app.main.ProductionConfigValidator": MagicMock(),
    "orchestrator.app.main.TenantScopeVerifier": MagicMock(),
    "orchestrator.app.main._validate_startup_security": lambda: MagicMock(),
    "orchestrator.app.main.configure_telemetry": lambda: None,
    "orchestrator.app.main.configure_metrics": lambda: None,
    "orchestrator.app.main.init_checkpointer": AsyncMock(),
    "orchestrator.app.main.initialize_ingestion_graph": lambda: None,
    "orchestrator.app.main.init_driver": lambda: None,
    "orchestrator.app.main.set_ingestion_semaphore": lambda _: None,
    "orchestrator.app.main.create_ingestion_semaphore": lambda **_kw: None,
    "orchestrator.app.main.RateLimitConfig": MagicMock(),
    "orchestrator.app.main._warn_insecure_auth": lambda _: None,
    "orchestrator.app.main._kafka_consumer_enabled": lambda: False,
    "orchestrator.app.main.flush_coalescing_outbox": lambda: 0,
    "orchestrator.app.main.drain_vector_outbox_sync": lambda: 0,
    "orchestrator.app.main.shutdown_ast_pool": lambda: None,
    "orchestrator.app.main.shutdown_pool": lambda: None,
    "orchestrator.app.main.shutdown_thread_pool": lambda: None,
    "orchestrator.app.main.close_driver": AsyncMock(),
    "orchestrator.app.main.close_checkpointer": AsyncMock(),
}


class TestValidateCalledDuringLifespanStartup:

    @pytest.mark.asyncio
    async def test_validate_called_during_lifespan_startup(self) -> None:
        from orchestrator.app.main import lifespan

        mock_validate = MagicMock()
        patches = {
            **_LIFESPAN_PATCHES,
            "orchestrator.app.main.validate_production_sync_mode": mock_validate,
        }
        with patch.dict(os.environ, {"DEPLOYMENT_MODE": "dev"}, clear=False):
            ctx = {}
            for target, replacement in patches.items():
                p = patch(target, replacement)
                ctx[target] = p.start()
            try:
                async with lifespan(MagicMock()):
                    mock_validate.assert_called_once()
            finally:
                patch.stopall()
