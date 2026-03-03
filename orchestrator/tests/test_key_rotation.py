"""Tests for SEC-11 (key rotation) and SEC-12 (HMAC request signing).

Validates dual-key token verification during rotation windows,
HMAC-SHA256 request signing for Go->Python calls, and replay
attack prevention via nonce + timestamp validation.
"""

import hashlib
import hmac
import time
import uuid

import jwt
import pytest

from orchestrator.app.access_control import (
    JWT_ALGORITHM,
    InvalidTokenError,
    KeyRotationConfig,
    sign_token,
    verify_token_with_rotation,
)
from orchestrator.app.request_signing import (
    verify_request_signature,
)


class TestDualKeyTokenVerification:
    def test_accepts_token_signed_with_current_key(self) -> None:
        config = KeyRotationConfig(
            current_key="current-secret-key-32bytes000000",
            previous_key="previous-secret-key-32bytes00000",
            overlap_window_seconds=300,
        )
        token = sign_token(
            {"team": "platform", "role": "admin"},
            config.current_key,
        )
        claims = verify_token_with_rotation(token, config)
        assert claims["team"] == "platform"
        assert claims["role"] == "admin"

    def test_accepts_token_signed_with_previous_key(self) -> None:
        config = KeyRotationConfig(
            current_key="current-secret-key-32bytes000000",
            previous_key="previous-secret-key-32bytes00000",
            overlap_window_seconds=300,
        )
        token = sign_token(
            {"team": "infra", "role": "viewer"},
            config.previous_key,
        )
        claims = verify_token_with_rotation(token, config)
        assert claims["team"] == "infra"
        assert claims["role"] == "viewer"

    def test_rejects_token_signed_with_unknown_key(self) -> None:
        config = KeyRotationConfig(
            current_key="current-secret-key-32bytes000000",
            previous_key="previous-secret-key-32bytes00000",
            overlap_window_seconds=300,
        )
        token = sign_token(
            {"team": "evil", "role": "admin"},
            "totally-unknown-key-32bytes00000",
        )
        with pytest.raises(InvalidTokenError):
            verify_token_with_rotation(token, config)

    def test_rejects_expired_token_even_with_valid_key(self) -> None:
        config = KeyRotationConfig(
            current_key="current-secret-key-32bytes000000",
            previous_key="previous-secret-key-32bytes00000",
            overlap_window_seconds=300,
        )
        expired_payload = {
            "team": "platform",
            "role": "admin",
            "iat": int(time.time()) - 7200,
            "exp": int(time.time()) - 3600,
        }
        token = jwt.encode(
            expired_payload,
            config.current_key,
            algorithm=JWT_ALGORITHM,
        )
        with pytest.raises(InvalidTokenError):
            verify_token_with_rotation(token, config)


class TestKeyRotationConfig:
    def test_config_is_frozen(self) -> None:
        config = KeyRotationConfig(
            current_key="current-secret-key-32bytes000000",
            previous_key="previous-secret-key-32bytes00000",
            overlap_window_seconds=300,
        )
        with pytest.raises(AttributeError):
            config.current_key = "new-key"  # type: ignore[misc]

    def test_overlap_window_configurable(self) -> None:
        config = KeyRotationConfig(
            current_key="current-secret-key-32bytes000000",
            previous_key="previous-secret-key-32bytes00000",
            overlap_window_seconds=600,
        )
        assert config.overlap_window_seconds == 600

    def test_previous_key_can_be_empty(self) -> None:
        config = KeyRotationConfig(
            current_key="current-secret-key-32bytes000000",
            previous_key="",
            overlap_window_seconds=300,
        )
        token = sign_token({"team": "t"}, config.current_key)
        claims = verify_token_with_rotation(token, config)
        assert claims["team"] == "t"


class TestHMACRequestSignatureVerification:
    def _sign_request(
        self,
        method: str,
        path: str,
        body: bytes,
        timestamp: str,
        nonce: str,
        secret: str,
    ) -> str:
        canonical = f"{method}\n{path}\n{timestamp}\n{nonce}\n".encode() + body
        return hmac.new(
            secret.encode(), canonical, hashlib.sha256
        ).hexdigest()

    def test_valid_signature_accepted(self) -> None:
        secret = "shared-secret-for-hmac-32bytes00"
        method = "POST"
        path = "/ingest"
        body = b'{"documents": []}'
        ts = str(int(time.time()))
        nonce = str(uuid.uuid4())
        sig = self._sign_request(method, path, body, ts, nonce, secret)

        result = verify_request_signature(
            method=method,
            path=path,
            body=body,
            timestamp=ts,
            nonce=nonce,
            signature=sig,
            secret=secret,
        )
        assert result is True

    def test_invalid_signature_rejected(self) -> None:
        secret = "shared-secret-for-hmac-32bytes00"
        ts = str(int(time.time()))
        nonce = str(uuid.uuid4())

        result = verify_request_signature(
            method="POST",
            path="/ingest",
            body=b'{"documents": []}',
            timestamp=ts,
            nonce=nonce,
            signature="deadbeef" * 8,
            secret=secret,
        )
        assert result is False

    def test_tampered_body_rejected(self) -> None:
        secret = "shared-secret-for-hmac-32bytes00"
        method = "POST"
        path = "/ingest"
        original_body = b'{"documents": []}'
        ts = str(int(time.time()))
        nonce = str(uuid.uuid4())
        sig = self._sign_request(
            method, path, original_body, ts, nonce, secret
        )

        result = verify_request_signature(
            method=method,
            path=path,
            body=b'{"documents": [{"evil": true}]}',
            timestamp=ts,
            nonce=nonce,
            signature=sig,
            secret=secret,
        )
        assert result is False


class TestReplayPrevention:
    def test_expired_timestamp_rejected(self) -> None:
        secret = "shared-secret-for-hmac-32bytes00"
        method = "POST"
        path = "/ingest"
        body = b'{"documents": []}'
        old_ts = str(int(time.time()) - 400)
        nonce = str(uuid.uuid4())
        sig = self._sign_request(method, path, body, old_ts, nonce, secret)

        result = verify_request_signature(
            method=method,
            path=path,
            body=body,
            timestamp=old_ts,
            nonce=nonce,
            signature=sig,
            secret=secret,
        )
        assert result is False

    def test_nonce_reuse_rejected(self) -> None:
        secret = "shared-secret-for-hmac-32bytes00"
        method = "POST"
        path = "/ingest"
        body = b'{"documents": []}'
        ts = str(int(time.time()))
        nonce = str(uuid.uuid4())
        sig = self._sign_request(method, path, body, ts, nonce, secret)

        first = verify_request_signature(
            method=method,
            path=path,
            body=body,
            timestamp=ts,
            nonce=nonce,
            signature=sig,
            secret=secret,
        )
        assert first is True

        second = verify_request_signature(
            method=method,
            path=path,
            body=body,
            timestamp=ts,
            nonce=nonce,
            signature=sig,
            secret=secret,
        )
        assert second is False

    def _sign_request(
        self,
        method: str,
        path: str,
        body: bytes,
        timestamp: str,
        nonce: str,
        secret: str,
    ) -> str:
        canonical = f"{method}\n{path}\n{timestamp}\n{nonce}\n".encode() + body
        return hmac.new(
            secret.encode(), canonical, hashlib.sha256
        ).hexdigest()
