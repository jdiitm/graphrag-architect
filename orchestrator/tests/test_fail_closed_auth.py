from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestAuthDefaultsFailClosed:

    def test_require_tokens_defaults_true(self) -> None:
        from orchestrator.app.config import AuthConfig

        for key in ("AUTH_REQUIRE_TOKENS", "AUTH_TOKEN_SECRET", "DEPLOYMENT_MODE"):
            os.environ.pop(key, None)

        config = AuthConfig.from_env()
        assert config.require_tokens is True, (
            "require_tokens must default to True (fail-closed). "
            "Developers must explicitly set AUTH_REQUIRE_TOKENS=false."
        )

    def test_explicit_false_disables_require_tokens(self) -> None:
        from orchestrator.app.config import AuthConfig

        env = {
            "AUTH_REQUIRE_TOKENS": "false",
            "DEPLOYMENT_MODE": "dev",
        }
        with patch.dict("os.environ", env, clear=False):
            os.environ.pop("AUTH_TOKEN_SECRET", None)
            config = AuthConfig.from_env()

        assert config.require_tokens is False

    def test_production_without_secret_exits(self) -> None:
        from orchestrator.app.config import AuthConfig

        env = {
            "DEPLOYMENT_MODE": "production",
        }
        with patch.dict("os.environ", env, clear=False):
            os.environ.pop("AUTH_TOKEN_SECRET", None)
            with pytest.raises(SystemExit):
                AuthConfig.from_env()

    def test_dev_without_secret_and_require_true_hard_rejects(self) -> None:
        from orchestrator.app.config import AuthConfig

        for key in ("AUTH_TOKEN_SECRET", "AUTH_REQUIRE_TOKENS"):
            os.environ.pop(key, None)

        config = AuthConfig.from_env()
        assert config.require_tokens is True
        assert config.token_secret == ""


class TestStartupHealthCheck:

    @pytest.mark.asyncio
    async def test_startup_rejects_when_require_true_no_secret(self) -> None:
        from orchestrator.app.main import app

        env = {
            "NEO4J_PASSWORD": "test",
            "NEO4J_URI": "bolt://localhost:7687",
            "GOOGLE_API_KEY": "test-key",
            "DEPLOYMENT_MODE": "dev",
        }

        for key in ("AUTH_TOKEN_SECRET", "AUTH_REQUIRE_TOKENS"):
            os.environ.pop(key, None)

        with (
            patch.dict("os.environ", env),
            patch("orchestrator.app.main.init_driver"),
            patch("orchestrator.app.main.close_driver", new_callable=AsyncMock),
            patch("orchestrator.app.main.init_checkpointer"),
            patch("orchestrator.app.main.close_checkpointer"),
            patch("orchestrator.app.main.configure_telemetry"),
            patch("orchestrator.app.main.configure_metrics"),
            patch("orchestrator.app.main.shutdown_pool"),
        ):
            startup_error = None
            try:
                async with app.router.lifespan_context(app):
                    pass
            except (SystemExit, RuntimeError) as exc:
                startup_error = exc

        assert startup_error is not None, (
            "App must refuse to start when require_tokens=true but no "
            "AUTH_TOKEN_SECRET is configured."
        )
