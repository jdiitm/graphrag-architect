from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULT_ENCODING_NAME = "cl100k_base"

_encoding_cache: dict[str, object] = {}


def _get_encoding(encoding_name: str = _DEFAULT_ENCODING_NAME) -> Optional[object]:
    if encoding_name in _encoding_cache:
        return _encoding_cache[encoding_name]
    try:
        import tiktoken
        enc = tiktoken.get_encoding(encoding_name)
        _encoding_cache[encoding_name] = enc
        return enc
    except (ImportError, Exception):
        logger.debug("tiktoken unavailable, falling back to heuristic")
        return None


def count_tokens(
    text: str,
    encoding_name: str = _DEFAULT_ENCODING_NAME,
) -> int:
    if not text:
        return 0
    enc = _get_encoding(encoding_name)
    if enc is not None:
        return len(enc.encode(text))
    return max(1, len(text) // 4)


def estimate_tokens_fast(text: str) -> int:
    return max(1, len(text) // 4)
