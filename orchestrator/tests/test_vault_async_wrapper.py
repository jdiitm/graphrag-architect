"""Tests for Vault async wrappers."""
from unittest.mock import patch

import pytest

from orchestrator.app.vault_provider import VaultSecretProvider, async_fetch_secret


class TestAsyncFetchSecret:
    @pytest.mark.asyncio
    async def test_async_fetch_delegates_to_sync(self) -> None:
        provider = VaultSecretProvider(vault_addr="")
        with patch(
            "orchestrator.app.vault_provider.VaultSecretProvider.fetch_secret",
            return_value="my-secret",
        ) as mock:
            result = await async_fetch_secret(provider, "path/to/secret", "API_KEY")
        mock.assert_called_once_with("path/to/secret", "API_KEY")
        assert result == "my-secret"

    @pytest.mark.asyncio
    async def test_async_fetch_runs_in_thread(self) -> None:
        provider = VaultSecretProvider(vault_addr="")
        with patch(
            "orchestrator.app.vault_provider.VaultSecretProvider.fetch_secret",
            return_value="threaded-val",
        ) as mock:
            result = await async_fetch_secret(provider, "p", "k")
        mock.assert_called_once()
        assert result == "threaded-val"
