"""
Tests for AuthConfig.assert_secure() — the startup-time security guard
that prevents the application from running in an insecure configuration.
"""
from __future__ import annotations

import pytest

from orchestrator.app.config import AuthConfig, InsecureConfigurationError


class TestAuthConfigAssertSecureRequireTokensMode:
    def test_raises_when_require_tokens_true_and_no_secret(self) -> None:
        cfg = AuthConfig(require_tokens=True, token_secret="")
        with pytest.raises(InsecureConfigurationError, match="AUTH_TOKEN_SECRET"):
            cfg.assert_secure()

    def test_does_not_raise_when_require_tokens_true_and_secret_present(self) -> None:
        cfg = AuthConfig(require_tokens=True, token_secret="super-secret-key")
        cfg.assert_secure()

    def test_does_not_raise_when_require_tokens_false_and_no_secret(self) -> None:
        cfg = AuthConfig(require_tokens=False, token_secret="")
        cfg.assert_secure()

    def test_does_not_raise_when_require_tokens_false_and_secret_present(self) -> None:
        cfg = AuthConfig(require_tokens=False, token_secret="some-secret")
        cfg.assert_secure()


class TestAuthConfigAssertSecureSecretValidation:
    def test_raises_when_require_tokens_true_and_whitespace_only_secret(self) -> None:
        cfg = AuthConfig(require_tokens=True, token_secret="   ")
        with pytest.raises(InsecureConfigurationError, match="AUTH_TOKEN_SECRET"):
            cfg.assert_secure()

    def test_raises_when_require_tokens_true_and_single_char_secret(self) -> None:
        cfg = AuthConfig(require_tokens=True, token_secret="x")
        with pytest.raises(InsecureConfigurationError, match="minimum length"):
            cfg.assert_secure()

    def test_secret_of_exactly_minimum_length_passes(self) -> None:
        cfg = AuthConfig(require_tokens=True, token_secret="a" * 16)
        cfg.assert_secure()

    def test_secret_below_minimum_length_raises(self) -> None:
        cfg = AuthConfig(require_tokens=True, token_secret="a" * 15)
        with pytest.raises(InsecureConfigurationError, match="minimum length"):
            cfg.assert_secure()
