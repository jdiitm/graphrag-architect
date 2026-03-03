from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.main import app


@pytest.mark.asyncio
async def test_lifespan_initializes_ingestion_graph_after_checkpointer() -> None:
    from orchestrator.app.main import _STATE

    calls: list[str] = []
    prior_semaphore = _STATE.get("semaphore")

    async def _init_checkpointer() -> None:
        calls.append("init_checkpointer")

    def _init_graph() -> object:
        calls.append("initialize_ingestion_graph")
        return object()

    try:
        with (
            patch("orchestrator.app.main.ProductionConfigValidator.from_env") as mock_validator,
            patch("orchestrator.app.main.TenantScopeVerifier.enforce_startup"),
            patch("orchestrator.app.main._validate_startup_security"),
            patch("orchestrator.app.main.configure_telemetry"),
            patch("orchestrator.app.main.configure_metrics"),
            patch("orchestrator.app.main.init_checkpointer", side_effect=_init_checkpointer),
            patch("orchestrator.app.main.initialize_ingestion_graph", side_effect=_init_graph),
            patch("orchestrator.app.main.init_driver"),
            patch("orchestrator.app.main.create_ingestion_semaphore"),
            patch("orchestrator.app.main._warn_insecure_auth"),
            patch("orchestrator.app.main._kafka_consumer_enabled", return_value=False),
            patch("orchestrator.app.main.close_driver", new_callable=AsyncMock),
            patch("orchestrator.app.main.close_checkpointer", new_callable=AsyncMock),
        ):
            mock_validator.return_value.validate_production_invariants.return_value = None
            async with app.router.lifespan_context(app):
                pass
    finally:
        _STATE["semaphore"] = prior_semaphore

    assert calls == ["init_checkpointer", "initialize_ingestion_graph"]
