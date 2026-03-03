from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_SA_TOKEN_PATH = (
    "/var/run/secrets/kubernetes.io/serviceaccount/token"
)


@dataclass(frozen=True)
class VaultSecretProvider:
    vault_addr: str = ""
    auth_method: str = "kubernetes"
    cache_ttl_seconds: int = 300
    vault_role: str = "graphrag"
    sa_token_path: str = _DEFAULT_SA_TOKEN_PATH
    _cache: dict[str, tuple[str, float]] = field(
        default_factory=dict, repr=False, compare=False,
    )

    @classmethod
    def from_env(cls) -> VaultSecretProvider:
        return cls(
            vault_addr=os.environ.get("VAULT_ADDR", ""),
            auth_method=os.environ.get("VAULT_AUTH_METHOD", "kubernetes"),
            cache_ttl_seconds=int(
                os.environ.get("VAULT_CACHE_TTL", "300"),
            ),
            vault_role=os.environ.get("VAULT_ROLE", "graphrag"),
            sa_token_path=os.environ.get(
                "VAULT_SA_TOKEN_PATH", _DEFAULT_SA_TOKEN_PATH,
            ),
        )

    def fetch_secret(self, path: str, key: str) -> str:
        if not self.vault_addr:
            return os.environ.get(key, "")

        cache_key = f"{path}:{key}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            value, ts = cached
            if time.monotonic() - ts < self.cache_ttl_seconds:
                return value

        try:
            value = self._vault_read(path, key)
        except Exception:
            logger.warning(
                "Vault read failed for %s/%s, falling back to env",
                path, key,
            )
            return os.environ.get(key, "")

        self._cache[cache_key] = (value, time.monotonic())
        return value

    def _vault_read(self, path: str, key: str) -> str:
        try:
            import requests  # noqa: C0415
        except ImportError:
            logger.error("requests library required for Vault access")
            return os.environ.get(key, "")

        token = self._authenticate()
        if not token:
            return os.environ.get(key, "")

        headers = {"X-Vault-Token": token}
        url = f"{self.vault_addr}/v1/{path}"
        response = requests.get(url, headers=headers, timeout=5)
        response.raise_for_status()
        data = response.json()
        return str(data.get("data", {}).get("data", {}).get(key, ""))

    def _authenticate(self) -> str:
        if self.auth_method == "kubernetes":
            return self._k8s_auth()
        return ""

    def _k8s_auth(self) -> str:
        sa_token = self._read_sa_token()
        if not sa_token:
            return ""

        try:
            import requests  # noqa: C0415
        except ImportError:
            return ""

        payload = {
            "role": self.vault_role,
            "jwt": sa_token,
        }
        url = f"{self.vault_addr}/v1/auth/kubernetes/login"
        response = requests.post(url, json=payload, timeout=5)
        response.raise_for_status()
        return str(response.json().get("auth", {}).get("client_token", ""))

    def _read_sa_token(self) -> str:
        try:
            with open(self.sa_token_path, encoding="utf-8") as fh:
                return fh.read().strip()
        except (FileNotFoundError, PermissionError):
            logger.debug(
                "Service account token not found at %s", self.sa_token_path,
            )
            return ""

    @staticmethod
    def is_secret_rotated(
        old_metadata: dict[str, Any],
        new_metadata: dict[str, Any],
    ) -> bool:
        old_version = old_metadata.get("version")
        new_version = new_metadata.get("version")
        return bool(old_version != new_version)
