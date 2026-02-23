import os
from unittest.mock import patch

import pytest

from orchestrator.app.config import AuthConfig


class TestAuthConfigDeploymentMode:
    @patch.dict(
        "os.environ",
        {"DEPLOYMENT_MODE": "production"},
        clear=False,
    )
    def test_production_mode_without_secret_raises_system_exit(self):
        os.environ.pop("AUTH_TOKEN_SECRET", None)
        with pytest.raises(SystemExit):
            AuthConfig.from_env()

    @patch.dict(
        "os.environ",
        {"DEPLOYMENT_MODE": "production", "AUTH_TOKEN_SECRET": "s3cr3t"},
        clear=False,
    )
    def test_production_mode_with_secret_succeeds(self):
        config = AuthConfig.from_env()
        assert config.token_secret == "s3cr3t"
        assert config.deployment_mode == "production"

    @patch.dict(
        "os.environ",
        {"DEPLOYMENT_MODE": "dev"},
        clear=False,
    )
    def test_dev_mode_without_secret_succeeds(self):
        os.environ.pop("AUTH_TOKEN_SECRET", None)
        config = AuthConfig.from_env()
        assert config.token_secret == ""
        assert config.deployment_mode == "dev"

    @patch.dict("os.environ", {}, clear=False)
    def test_default_mode_is_dev(self):
        os.environ.pop("DEPLOYMENT_MODE", None)
        os.environ.pop("AUTH_TOKEN_SECRET", None)
        config = AuthConfig.from_env()
        assert config.deployment_mode == "dev"

    @patch.dict(
        "os.environ",
        {"DEPLOYMENT_MODE": "production", "AUTH_TOKEN_SECRET": "key"},
        clear=False,
    )
    def test_production_mode_sets_require_tokens_implicitly(self):
        os.environ.pop("AUTH_REQUIRE_TOKENS", None)
        config = AuthConfig.from_env()
        assert config.require_tokens is True

    @patch.dict(
        "os.environ",
        {"DEPLOYMENT_MODE": "dev", "AUTH_REQUIRE_TOKENS": "true"},
        clear=False,
    )
    def test_dev_mode_respects_explicit_require_tokens(self):
        os.environ.pop("AUTH_TOKEN_SECRET", None)
        config = AuthConfig.from_env()
        assert config.require_tokens is True

    @pytest.mark.parametrize("bad_mode", ["prod", "staging", "test", "prd", "live"])
    def test_unknown_deployment_mode_raises_system_exit(self, bad_mode):
        with patch.dict("os.environ", {"DEPLOYMENT_MODE": bad_mode}, clear=False):
            with pytest.raises(SystemExit, match="not recognized"):
                AuthConfig.from_env()


class TestLifespanStartupGuard:
    @patch.dict(
        "os.environ",
        {"DEPLOYMENT_MODE": "production"},
        clear=False,
    )
    def test_lifespan_crashes_before_accepting_connections_in_production(self):
        os.environ.pop("AUTH_TOKEN_SECRET", None)
        os.environ.pop("NEO4J_PASSWORD", None)
        from orchestrator.app.main import _validate_startup_security
        with pytest.raises(SystemExit):
            _validate_startup_security()

    @patch.dict(
        "os.environ",
        {"DEPLOYMENT_MODE": "dev"},
        clear=False,
    )
    def test_lifespan_does_not_crash_in_dev_mode(self):
        os.environ.pop("AUTH_TOKEN_SECRET", None)
        from orchestrator.app.main import _validate_startup_security
        _validate_startup_security()
