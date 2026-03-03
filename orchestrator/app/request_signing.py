from __future__ import annotations

import hashlib
import hmac
import time
from collections import OrderedDict
from typing import Optional

_MAX_TIMESTAMP_DRIFT_SECONDS = 300
_NONCE_CACHE_MAX_SIZE = 10_000


class _NonceCache:
    def __init__(self, maxsize: int = _NONCE_CACHE_MAX_SIZE) -> None:
        self._seen: OrderedDict[str, float] = OrderedDict()
        self._maxsize = maxsize

    def check_and_record(self, nonce: str) -> bool:
        now = time.time()
        self._evict_expired(now)

        if nonce in self._seen:
            return False

        self._seen[nonce] = now
        if len(self._seen) > self._maxsize:
            self._seen.popitem(last=False)
        return True

    def _evict_expired(self, now: float) -> None:
        cutoff = now - _MAX_TIMESTAMP_DRIFT_SECONDS
        while self._seen:
            oldest_key = next(iter(self._seen))
            if self._seen[oldest_key] < cutoff:
                self._seen.pop(oldest_key)
            else:
                break


_nonce_cache = _NonceCache()


def _compute_signature(
    method: str,
    path: str,
    body: bytes,
    timestamp: str,
    nonce: str,
    secret: str,
) -> str:
    canonical = f"{method}\n{path}\n{timestamp}\n{nonce}\n".encode() + body
    return hmac.new(secret.encode(), canonical, hashlib.sha256).hexdigest()


def verify_request_signature(
    method: str,
    path: str,
    body: bytes,
    timestamp: str,
    nonce: str,
    signature: str,
    secret: str,
    _cache: Optional[_NonceCache] = None,
) -> bool:
    try:
        ts = int(timestamp)
    except (ValueError, TypeError):
        return False

    if abs(time.time() - ts) > _MAX_TIMESTAMP_DRIFT_SECONDS:
        return False

    cache = _cache or _nonce_cache
    if not cache.check_and_record(nonce):
        return False

    expected = _compute_signature(method, path, body, timestamp, nonce, secret)
    return hmac.compare_digest(expected, signature)
