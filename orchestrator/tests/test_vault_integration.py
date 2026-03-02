from __future__ import annotations

import json
import time
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from orchestrator.app.vault_provider import VaultSecretProvider

_VAULT_RESPONSE_V1 = json.dumps({
    "data": {
        "data": {"MY_SECRET": "vault-val", "OTHER": "other-val"},
        "metadata": {"version": 1},
    },
}).encode()

_VAULT_RESPONSE_V2 = json.dumps({
    "data": {
        "data": {"MY_SECRET": "rotated-val", "OTHER": "other-v2"},
        "metadata": {"version": 2},
    },
}).encode()

_AUTH_RESPONSE = json.dumps({
    "auth": {"client_token": "s.fake-token"},
}).encode()


def _mock_urlopen_factory(vault_payload: bytes) -> MagicMock:
    call_count = [0]

    def _side_effect(req: object, timeout: int = 5) -> BytesIO:
        call_count[0] += 1
        url = getattr(req, "full_url", str(req))
        if "auth/kubernetes/login" in url:
            return BytesIO(_AUTH_RESPONSE)
        return BytesIO(vault_payload)

    mock = MagicMock(side_effect=_side_effect)
    mock.call_count_ref = call_count
    return mock


class TestVaultSecretProviderExists:

    def test_class_is_importable(self) -> None:
        assert VaultSecretProvider is not None

    def test_has_fetch_secret_method(self) -> None:
        provider = VaultSecretProvider.from_env()
        assert callable(getattr(provider, "fetch_secret", None))

    def test_frozen_dataclass(self) -> None:
        provider = VaultSecretProvider.from_env()
        with pytest.raises(AttributeError):
            provider.vault_addr = "http://changed"  # type: ignore[misc]


class TestVaultFromEnv:

    def test_from_env_reads_vault_addr(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VAULT_ADDR", "http://vault.test:8200")
        provider = VaultSecretProvider.from_env()
        assert provider.vault_addr == "http://vault.test:8200"

    def test_from_env_reads_vault_role(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VAULT_ROLE", "custom-role")
        provider = VaultSecretProvider.from_env()
        assert provider.vault_role == "custom-role"

    def test_from_env_default_role(self) -> None:
        provider = VaultSecretProvider.from_env()
        assert provider.vault_role == "graphrag"

    def test_from_env_reads_cache_ttl(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VAULT_CACHE_TTL_SECONDS", "120")
        provider = VaultSecretProvider.from_env()
        assert provider.cache_ttl_seconds == 120

    def test_from_env_default_cache_ttl(self) -> None:
        provider = VaultSecretProvider.from_env()
        assert provider.cache_ttl_seconds == 300

    def test_from_env_reads_token_path(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VAULT_K8S_TOKEN_PATH", "/custom/token")
        provider = VaultSecretProvider.from_env()
        assert provider.k8s_token_path == "/custom/token"


class TestVaultFallbackToEnvVars:

    def test_returns_env_var_when_vault_unavailable(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("VAULT_ADDR", raising=False)
        monkeypatch.setenv("NEO4J_PASSWORD", "env-secret-value")
        provider = VaultSecretProvider.from_env()
        result = provider.fetch_secret("secret/data/graphrag", "NEO4J_PASSWORD")
        assert result == "env-secret-value"

    def test_returns_empty_string_when_neither_vault_nor_env(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("VAULT_ADDR", raising=False)
        monkeypatch.delenv("NONEXISTENT_KEY", raising=False)
        provider = VaultSecretProvider.from_env()
        result = provider.fetch_secret("secret/data/graphrag", "NONEXISTENT_KEY")
        assert result == ""

    def test_fallback_logs_warning(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.delenv("VAULT_ADDR", raising=False)
        monkeypatch.setenv("SOME_KEY", "fallback")
        provider = VaultSecretProvider.from_env()
        with caplog.at_level("WARNING"):
            provider.fetch_secret("secret/data/graphrag", "SOME_KEY")
        assert any("fallback" in r.message.lower() for r in caplog.records)


class TestVaultKubernetesAuth:

    def test_default_token_path(self) -> None:
        provider = VaultSecretProvider.from_env()
        assert provider.k8s_token_path == (
            "/var/run/secrets/kubernetes.io/serviceaccount/token"
        )

    def test_custom_token_path(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VAULT_K8S_TOKEN_PATH", "/custom/token")
        provider = VaultSecretProvider.from_env()
        assert provider.k8s_token_path == "/custom/token"

    def test_auth_method_is_kubernetes(self) -> None:
        provider = VaultSecretProvider.from_env()
        assert provider.auth_method == "kubernetes"


class TestVaultSecretCaching:

    def test_cached_secret_returned_within_ttl(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: object,
    ) -> None:
        monkeypatch.setenv("VAULT_ADDR", "http://vault.test:8200")
        monkeypatch.setenv("VAULT_CACHE_TTL_SECONDS", "60")
        token_file = tmp_path / "token"  # type: ignore[operator]
        token_file.write_text("fake-jwt")
        monkeypatch.setenv("VAULT_K8S_TOKEN_PATH", str(token_file))
        provider = VaultSecretProvider.from_env()

        with patch("urllib.request.urlopen", _mock_urlopen_factory(_VAULT_RESPONSE_V1)):
            first = provider.fetch_secret("secret/data/graphrag", "MY_SECRET")

        with patch("urllib.request.urlopen", _mock_urlopen_factory(_VAULT_RESPONSE_V2)):
            second = provider.fetch_secret("secret/data/graphrag", "MY_SECRET")

        assert first == "vault-val"
        assert second == "vault-val"

    def test_cache_expires_after_ttl(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: object,
    ) -> None:
        monkeypatch.setenv("VAULT_ADDR", "http://vault.test:8200")
        monkeypatch.setenv("VAULT_CACHE_TTL_SECONDS", "1")
        token_file = tmp_path / "token"  # type: ignore[operator]
        token_file.write_text("fake-jwt")
        monkeypatch.setenv("VAULT_K8S_TOKEN_PATH", str(token_file))
        provider = VaultSecretProvider.from_env()

        mock_v1 = _mock_urlopen_factory(_VAULT_RESPONSE_V1)
        with patch("urllib.request.urlopen", mock_v1):
            provider.fetch_secret("secret/data/graphrag", "MY_SECRET")

        time.sleep(1.1)

        mock_v2 = _mock_urlopen_factory(_VAULT_RESPONSE_V2)
        with patch("urllib.request.urlopen", mock_v2):
            result = provider.fetch_secret("secret/data/graphrag", "MY_SECRET")

        assert result == "rotated-val"

    def test_configurable_ttl_respected(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VAULT_CACHE_TTL_SECONDS", "600")
        provider = VaultSecretProvider.from_env()
        assert provider.cache_ttl_seconds == 600


class TestVaultSecretRotationDetection:

    def test_detects_version_change(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: object,
    ) -> None:
        monkeypatch.setenv("VAULT_ADDR", "http://vault.test:8200")
        token_file = tmp_path / "token"  # type: ignore[operator]
        token_file.write_text("fake-jwt")
        monkeypatch.setenv("VAULT_K8S_TOKEN_PATH", str(token_file))
        provider = VaultSecretProvider.from_env()

        with patch("urllib.request.urlopen", _mock_urlopen_factory(_VAULT_RESPONSE_V1)):
            provider.fetch_secret("secret/data/graphrag", "MY_SECRET")
        assert provider.get_cached_version("secret/data/graphrag") == 1

        provider.invalidate_cache("secret/data/graphrag")

        with patch("urllib.request.urlopen", _mock_urlopen_factory(_VAULT_RESPONSE_V2)):
            result = provider.fetch_secret("secret/data/graphrag", "MY_SECRET")
        assert result == "rotated-val"
        assert provider.get_cached_version("secret/data/graphrag") == 2

    def test_get_cached_version_returns_none_when_no_cache(self) -> None:
        provider = VaultSecretProvider.from_env()
        assert provider.get_cached_version("secret/data/nonexistent") is None

    def test_invalidate_cache_clears_entry(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: object,
    ) -> None:
        monkeypatch.setenv("VAULT_ADDR", "http://vault.test:8200")
        token_file = tmp_path / "token"  # type: ignore[operator]
        token_file.write_text("fake-jwt")
        monkeypatch.setenv("VAULT_K8S_TOKEN_PATH", str(token_file))
        provider = VaultSecretProvider.from_env()

        with patch("urllib.request.urlopen", _mock_urlopen_factory(_VAULT_RESPONSE_V1)):
            provider.fetch_secret("secret/data/graphrag", "MY_SECRET")
        assert provider.get_cached_version("secret/data/graphrag") == 1

        provider.invalidate_cache("secret/data/graphrag")
        assert provider.get_cached_version("secret/data/graphrag") is None
