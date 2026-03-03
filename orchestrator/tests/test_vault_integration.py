from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from orchestrator.app.vault_provider import VaultSecretProvider


class TestVaultSecretProviderExists:

    def test_class_exists(self) -> None:
        assert VaultSecretProvider is not None

    def test_is_frozen_dataclass(self) -> None:
        provider = VaultSecretProvider(
            vault_addr="http://vault:8200",
            auth_method="kubernetes",
        )
        with pytest.raises(AttributeError):
            provider.vault_addr = "http://other:8200"  # type: ignore[misc]

    def test_has_fetch_secret_method(self) -> None:
        provider = VaultSecretProvider(
            vault_addr="http://vault:8200",
            auth_method="kubernetes",
        )
        assert callable(getattr(provider, "fetch_secret", None))


class TestVaultSecretProviderFromEnv:

    def test_from_env_reads_vault_addr(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VAULT_ADDR", "http://vault.svc:8200")
        monkeypatch.setenv("VAULT_AUTH_METHOD", "kubernetes")
        provider = VaultSecretProvider.from_env()
        assert provider.vault_addr == "http://vault.svc:8200"

    def test_from_env_defaults_to_empty_addr(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("VAULT_ADDR", raising=False)
        monkeypatch.delenv("VAULT_AUTH_METHOD", raising=False)
        provider = VaultSecretProvider.from_env()
        assert provider.vault_addr == ""

    def test_from_env_reads_auth_method(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VAULT_ADDR", "http://vault:8200")
        monkeypatch.setenv("VAULT_AUTH_METHOD", "kubernetes")
        provider = VaultSecretProvider.from_env()
        assert provider.auth_method == "kubernetes"

    def test_from_env_default_auth_method(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("VAULT_AUTH_METHOD", raising=False)
        provider = VaultSecretProvider.from_env()
        assert provider.auth_method == "kubernetes"

    def test_from_env_reads_cache_ttl(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VAULT_ADDR", "http://vault:8200")
        monkeypatch.setenv("VAULT_CACHE_TTL", "120")
        provider = VaultSecretProvider.from_env()
        assert provider.cache_ttl_seconds == 120

    def test_from_env_default_cache_ttl(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("VAULT_CACHE_TTL", raising=False)
        provider = VaultSecretProvider.from_env()
        assert provider.cache_ttl_seconds == 300

    def test_from_env_reads_vault_role(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VAULT_ROLE", "graphrag-orchestrator")
        provider = VaultSecretProvider.from_env()
        assert provider.vault_role == "graphrag-orchestrator"

    def test_from_env_reads_token_path(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(
            "VAULT_SA_TOKEN_PATH",
            "/var/run/secrets/kubernetes.io/serviceaccount/token",
        )
        provider = VaultSecretProvider.from_env()
        assert provider.sa_token_path == (
            "/var/run/secrets/kubernetes.io/serviceaccount/token"
        )


class TestVaultFallbackToEnvVars:

    def test_fallback_when_vault_addr_empty(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("VAULT_ADDR", raising=False)
        provider = VaultSecretProvider.from_env()
        monkeypatch.setenv("NEO4J_PASSWORD", "from-env-var")
        result = provider.fetch_secret("secret/data/graphrag", "NEO4J_PASSWORD")
        assert result == "from-env-var"

    def test_fallback_returns_empty_when_env_missing(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("VAULT_ADDR", raising=False)
        monkeypatch.delenv("MISSING_KEY", raising=False)
        provider = VaultSecretProvider.from_env()
        result = provider.fetch_secret("secret/data/graphrag", "MISSING_KEY")
        assert result == ""


class TestVaultSecretCaching:

    def test_cached_value_returned_within_ttl(self) -> None:
        provider = VaultSecretProvider(
            vault_addr="http://vault:8200",
            auth_method="kubernetes",
            cache_ttl_seconds=300,
        )
        with patch.object(
            VaultSecretProvider, "_vault_read", return_value="cached-secret",
        ):
            first = provider.fetch_secret("secret/data/db", "password")
        call_count = MagicMock(side_effect=ConnectionError("vault down"))
        with patch.object(VaultSecretProvider, "_vault_read", call_count):
            second = provider.fetch_secret("secret/data/db", "password")
        assert first == second == "cached-secret"
        call_count.assert_not_called()

    def test_cache_expires_after_ttl(self) -> None:
        provider = VaultSecretProvider(
            vault_addr="http://vault:8200",
            auth_method="kubernetes",
            cache_ttl_seconds=0,
        )
        with patch.object(
            VaultSecretProvider, "_vault_read", return_value="v1",
        ):
            first = provider.fetch_secret("secret/data/db", "password")
        time.sleep(0.01)
        with patch.object(
            VaultSecretProvider, "_vault_read", return_value="v2",
        ):
            second = provider.fetch_secret("secret/data/db", "password")
        assert first == "v1"
        assert second == "v2"


class TestVaultKubernetesAuth:

    def test_reads_service_account_token(self, tmp_path: object) -> None:
        import pathlib

        token_file = pathlib.Path(str(tmp_path)) / "token"
        token_file.write_text("eyJhbGciOiJSUzI1NiIsImtpZCI6IjEifQ.test.sig")
        provider = VaultSecretProvider(
            vault_addr="http://vault:8200",
            auth_method="kubernetes",
            sa_token_path=str(token_file),
        )
        token = provider._read_sa_token()
        assert token == "eyJhbGciOiJSUzI1NiIsImtpZCI6IjEifQ.test.sig"

    def test_returns_empty_when_token_file_missing(self) -> None:
        provider = VaultSecretProvider(
            vault_addr="http://vault:8200",
            auth_method="kubernetes",
            sa_token_path="/nonexistent/path/token",
        )
        token = provider._read_sa_token()
        assert token == ""


class TestVaultSecretRotationDetection:

    def test_detects_version_change(self) -> None:
        provider = VaultSecretProvider(
            vault_addr="http://vault:8200",
            auth_method="kubernetes",
        )
        old_meta = {"version": 1}
        new_meta = {"version": 2}
        assert provider.is_secret_rotated(old_meta, new_meta) is True

    def test_no_rotation_same_version(self) -> None:
        provider = VaultSecretProvider(
            vault_addr="http://vault:8200",
            auth_method="kubernetes",
        )
        meta = {"version": 3}
        assert provider.is_secret_rotated(meta, meta) is False

    def test_handles_missing_version_key(self) -> None:
        provider = VaultSecretProvider(
            vault_addr="http://vault:8200",
            auth_method="kubernetes",
        )
        assert provider.is_secret_rotated({}, {"version": 1}) is True
        assert provider.is_secret_rotated({"version": 1}, {}) is True
