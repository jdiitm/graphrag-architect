from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException


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


class TestResolveTenantContextFailClosed:

    def test_no_secret_with_require_tokens_raises_503(self) -> None:
        from orchestrator.app.main import _resolve_tenant_context

        env = {
            "DEPLOYMENT_MODE": "dev",
            "AUTH_REQUIRE_TOKENS": "true",
        }
        with patch.dict("os.environ", env, clear=False):
            os.environ.pop("AUTH_TOKEN_SECRET", None)
            with pytest.raises(HTTPException) as exc_info:
                _resolve_tenant_context("Bearer some-token")
            assert exc_info.value.status_code == 503

    def test_no_secret_with_require_tokens_default_raises_503(self) -> None:
        from orchestrator.app.main import _resolve_tenant_context

        for key in ("AUTH_TOKEN_SECRET", "AUTH_REQUIRE_TOKENS"):
            os.environ.pop(key, None)
        env = {"DEPLOYMENT_MODE": "dev"}
        with patch.dict("os.environ", env, clear=False):
            with pytest.raises(HTTPException) as exc_info:
                _resolve_tenant_context("Bearer some-token")
            assert exc_info.value.status_code == 503

    def test_no_secret_no_require_allows_default_tenant(self) -> None:
        from orchestrator.app.main import _resolve_tenant_context
        from orchestrator.app.tenant_isolation import TenantContext

        env = {
            "DEPLOYMENT_MODE": "dev",
            "AUTH_REQUIRE_TOKENS": "false",
        }
        with patch.dict("os.environ", env, clear=False):
            os.environ.pop("AUTH_TOKEN_SECRET", None)
            result = _resolve_tenant_context(None)
            assert result.tenant_id == TenantContext.default().tenant_id

    def test_no_auth_header_with_require_tokens_raises_401(self) -> None:
        from fastapi import HTTPException
        from orchestrator.app.main import _resolve_tenant_context

        env = {
            "DEPLOYMENT_MODE": "dev",
            "AUTH_REQUIRE_TOKENS": "true",
        }
        with patch.dict("os.environ", env, clear=False):
            os.environ.pop("AUTH_TOKEN_SECRET", None)
            with pytest.raises(HTTPException) as exc_info:
                _resolve_tenant_context(None)
            assert exc_info.value.status_code == 401


class TestVerifyIngestAuthFailClosed:

    def test_no_secret_with_require_tokens_raises_503(self) -> None:
        from orchestrator.app.main import _verify_ingest_auth

        env = {
            "DEPLOYMENT_MODE": "dev",
            "AUTH_REQUIRE_TOKENS": "true",
        }
        with patch.dict("os.environ", env, clear=False):
            os.environ.pop("AUTH_TOKEN_SECRET", None)
            with pytest.raises(HTTPException) as exc_info:
                _verify_ingest_auth("Bearer some-token")
            assert exc_info.value.status_code == 503

    def test_no_secret_no_require_allows_through(self) -> None:
        from orchestrator.app.main import _verify_ingest_auth

        env = {
            "DEPLOYMENT_MODE": "dev",
            "AUTH_REQUIRE_TOKENS": "false",
        }
        with patch.dict("os.environ", env, clear=False):
            os.environ.pop("AUTH_TOKEN_SECRET", None)
            _verify_ingest_auth("Bearer some-token")


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
