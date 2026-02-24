from __future__ import annotations

from unittest.mock import patch

import pytest

from orchestrator.app.container import AppContainer


_CONTAINER_ENV = {
    "DEPLOYMENT_MODE": "dev",
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


class TestAppContainerFromEnv:
    def test_from_env_creates_valid_container(self) -> None:
        with patch.dict("os.environ", _CONTAINER_ENV):
            container = AppContainer.from_env()
        assert container.circuit_breaker is not None
        assert container.auth_config is not None
        assert container.extraction_config is not None

    def test_from_env_all_fields_populated(self) -> None:
        with patch.dict("os.environ", _CONTAINER_ENV):
            container = AppContainer.from_env()
        assert container.circuit_breaker.state.name in (
            "CLOSED",
            "OPEN",
            "HALF_OPEN",
        )
        assert container.auth_config.deployment_mode == "dev"
        assert container.auth_config.token_secret == ""
        assert container.extraction_config.google_api_key == "test-key"
        assert container.extraction_config.model_name != ""

    def test_container_is_frozen(self) -> None:
        from dataclasses import FrozenInstanceError

        with patch.dict("os.environ", _CONTAINER_ENV):
            container = AppContainer.from_env()
        with pytest.raises(FrozenInstanceError):
            setattr(container, "circuit_breaker", None)
