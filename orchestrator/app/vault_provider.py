from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)

_DEFAULT_K8S_TOKEN_PATH = (
    "/var/run/secrets/kubernetes.io/serviceaccount/token"
)
_DEFAULT_CACHE_TTL = 300


@dataclass(frozen=True)
class _CacheEntry:
    data: dict[str, str]
    version: Optional[int]
    timestamp: float


@dataclass(frozen=True)
class VaultSecretProvider:
    vault_addr: str = ""
    vault_role: str = "graphrag"
    k8s_token_path: str = _DEFAULT_K8S_TOKEN_PATH
    cache_ttl_seconds: int = _DEFAULT_CACHE_TTL
    auth_method: str = "kubernetes"
    _cache: dict[str, _CacheEntry] = field(
        default_factory=dict, repr=False, compare=False,
    )
    _client_token: list[str] = field(
        default_factory=list, repr=False, compare=False,
    )

    @classmethod
    def from_env(cls) -> VaultSecretProvider:
        return cls(
            vault_addr=os.environ.get("VAULT_ADDR", ""),
            vault_role=os.environ.get("VAULT_ROLE", "graphrag"),
            k8s_token_path=os.environ.get(
                "VAULT_K8S_TOKEN_PATH", _DEFAULT_K8S_TOKEN_PATH,
            ),
            cache_ttl_seconds=int(
                os.environ.get(
                    "VAULT_CACHE_TTL_SECONDS", str(_DEFAULT_CACHE_TTL),
                ),
            ),
        )

    @property
    def _vault_available(self) -> bool:
        return bool(self.vault_addr)

    def fetch_secret(self, path: str, key: str) -> str:
        if not self._vault_available:
            logger.warning(
                "Vault unavailable (VAULT_ADDR not set), "
                "using env-var fallback for %s",
                key,
            )
            return os.environ.get(key, "")

        cached = self._cache.get(path)
        if cached is not None:
            age = time.monotonic() - cached.timestamp
            if self.cache_ttl_seconds > 0 and age < self.cache_ttl_seconds:
                return cached.data.get(key, "")

        try:
            response = self._vault_read(path)
        except Exception:
            logger.warning(
                "Vault read failed for %s, falling back to "
                "environment variable for %s",
                path, key,
            )
            return os.environ.get(key, "")

        data = response.get("data", {})
        inner: dict[str, Any] = (
            data.get("data", {}) if isinstance(data, dict) else {}
        )
        metadata = (
            data.get("metadata", {}) if isinstance(data, dict) else {}
        )
        version = (
            metadata.get("version")
            if isinstance(metadata, dict) else None
        )

        secret_map: dict[str, str] = {
            k: str(v) for k, v in inner.items() if v is not None
        }
        self._cache[path] = _CacheEntry(
            data=secret_map,
            version=version,
            timestamp=time.monotonic(),
        )
        return secret_map.get(key, "")

    def get_cached_version(self, path: str) -> Optional[int]:
        cached = self._cache.get(path)
        if cached is None:
            return None
        return cached.version

    def invalidate_cache(self, path: str) -> None:
        self._cache.pop(path, None)

    def _vault_read(self, path: str) -> dict[str, Any]:
        token = self._get_client_token()
        url = f"{self.vault_addr}/v1/{path}"
        req = urllib.request.Request(
            url, headers={"X-Vault-Token": token},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            body: dict[str, Any] = json.loads(resp.read())
        return body

    def _get_client_token(self) -> str:
        if self._client_token:
            return self._client_token[0]

        with open(self.k8s_token_path, encoding="utf-8") as fh:
            jwt = fh.read().strip()

        url = f"{self.vault_addr}/v1/auth/kubernetes/login"
        payload = json.dumps(
            {"role": self.vault_role, "jwt": jwt},
        ).encode()
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            body: dict[str, Any] = json.loads(resp.read())

        auth_data = body.get("auth", {})
        client_token = str(auth_data["client_token"])
        self._client_token.append(client_token)
        return client_token
