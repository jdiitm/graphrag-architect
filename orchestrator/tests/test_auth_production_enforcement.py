from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.app.config import AuthConfig


class TestResolveContextProductionMode:
    def test_production_no_auth_returns_401(self):
        env = {
            "DEPLOYMENT_MODE": "production",
            "AUTH_TOKEN_SECRET": "supersecret",
            "AUTH_REQUIRE_TOKENS": "true",
            "NEO4J_PASSWORD": "password",
        }
        with patch.dict("os.environ", env, clear=False):
            from orchestrator.app.main import _resolve_tenant_context
            from fastapi import HTTPException
            with pytest.raises(HTTPException) as exc_info:
                _resolve_tenant_context(None)
            assert exc_info.value.status_code == 401

    def test_production_no_auth_header_empty_string_returns_401(self):
        env = {
            "DEPLOYMENT_MODE": "production",
            "AUTH_TOKEN_SECRET": "supersecret",
            "AUTH_REQUIRE_TOKENS": "true",
            "NEO4J_PASSWORD": "password",
        }
        with patch.dict("os.environ", env, clear=False):
            from orchestrator.app.main import _resolve_tenant_context
            from fastapi import HTTPException
            with pytest.raises(HTTPException) as exc_info:
                _resolve_tenant_context("")
            assert exc_info.value.status_code == 401


class TestDevModePermissive:
    def test_dev_no_auth_returns_default_tenant(self):
        env = {
            "DEPLOYMENT_MODE": "dev",
            "AUTH_TOKEN_SECRET": "",
            "AUTH_REQUIRE_TOKENS": "false",
            "NEO4J_PASSWORD": "password",
        }
        with patch.dict("os.environ", env, clear=False):
            from orchestrator.app.main import _resolve_tenant_context
            ctx = _resolve_tenant_context(None)
            assert ctx.tenant_id == ""

    def test_dev_empty_auth_returns_default_tenant(self):
        env = {
            "DEPLOYMENT_MODE": "dev",
            "AUTH_TOKEN_SECRET": "",
            "AUTH_REQUIRE_TOKENS": "false",
            "NEO4J_PASSWORD": "password",
        }
        with patch.dict("os.environ", env, clear=False):
            from orchestrator.app.main import _resolve_tenant_context
            ctx = _resolve_tenant_context("")
            assert ctx.tenant_id == ""


class TestProductionStartupGuard:
    def test_production_without_secret_exits(self):
        env = {
            "DEPLOYMENT_MODE": "production",
            "AUTH_TOKEN_SECRET": "",
        }
        with patch.dict("os.environ", env, clear=False):
            with pytest.raises(SystemExit):
                AuthConfig.from_env()
