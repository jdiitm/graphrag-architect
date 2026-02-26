from __future__ import annotations

import re
from typing import List, Tuple

_DEFAULT_MAX_QUERY_CHARS = 4_000
_DEFAULT_MAX_SOURCE_CHARS = 1_000_000

_INJECTION_PATTERNS: List[Tuple[re.Pattern[str], str]] = [
    (re.compile(
        r"ignore\s+(all\s+)?previous\s+(instructions?|rules?)", re.IGNORECASE,
    ), "[REDACTED]"),
    (re.compile(
        r"ignore\s+(all\s+)?prior\s+(instructions?|rules?)", re.IGNORECASE,
    ), "[REDACTED]"),
    (re.compile(
        r"ignore\s+(all\s+)?above\s+(instructions?|rules?)", re.IGNORECASE,
    ), "[REDACTED]"),
    (re.compile(r"disregard\s+(all\s+)?previous\s+instructions?", re.IGNORECASE), "[REDACTED]"),
    (re.compile(r"system\s*prompt\s*:", re.IGNORECASE), "[REDACTED]:"),
    (re.compile(r"^you\s+are\s+(an?\s+)?", re.IGNORECASE | re.MULTILINE), "[REDACTED] "),
    (re.compile(r"forget\s+(all\s+)?(your\s+)?instructions?", re.IGNORECASE), "[REDACTED]"),
    (re.compile(r"new\s+instructions?\s*:", re.IGNORECASE), "[REDACTED]:"),
    (re.compile(r"override\s+(system|safety|security)\s+", re.IGNORECASE), "[REDACTED] "),
    (re.compile(r"act\s+as\s+(if\s+)?you\s+(are|were)\s+", re.IGNORECASE), "[REDACTED] "),
    (re.compile(r"pretend\s+(that\s+)?you\s+(are|were)\s+", re.IGNORECASE), "[REDACTED] "),
]

_SECRET_PATTERNS: List[Tuple[re.Pattern[str], str]] = [
    (re.compile(r"sk-[A-Za-z0-9]{20,}"), "[REDACTED_SECRET]"),
    (re.compile(r"AKIA[A-Z0-9]{16}"), "[REDACTED_SECRET]"),
    (re.compile(r"ghp_[A-Za-z0-9]{36,}"), "[REDACTED_SECRET]"),
    (re.compile(r"ghs_[A-Za-z0-9]{36,}"), "[REDACTED_SECRET]"),
    (re.compile(r"-----BEGIN[A-Z ]*PRIVATE KEY-----[\s\S]*?-----END[A-Z ]*PRIVATE KEY-----"),
     "[REDACTED_SECRET]"),
    (re.compile(r"-----BEGIN[A-Z ]*KEY-----[\s\S]*?-----END[A-Z ]*KEY-----"),
     "[REDACTED_SECRET]"),
    (re.compile(
        r"""(?:['"])((?:sk-|AKIA|ghp_|ghs_)[A-Za-z0-9+/=]{16,})(?:['"])"""),
     '"[REDACTED_SECRET]"'),
]

_XML_BOUNDARY_PATTERN = re.compile(
    r"<\s*/?\s*(?:graph_context|user_query|system|assistant)\s*>",
    re.IGNORECASE,
)

_CONTROL_CHAR_PATTERN = re.compile(
    r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]"
)


def _strip_control_chars(text: str) -> str:
    return _CONTROL_CHAR_PATTERN.sub("", text)


def _strip_xml_boundaries(text: str) -> str:
    return _XML_BOUNDARY_PATTERN.sub("", text)


def _apply_injection_filters(text: str) -> str:
    for pattern, replacement in _INJECTION_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def _apply_secret_filters(text: str) -> str:
    for pattern, replacement in _SECRET_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def sanitize_query_input(
    raw: str,
    max_chars: int = _DEFAULT_MAX_QUERY_CHARS,
) -> str:
    cleaned = _strip_control_chars(raw)
    cleaned = cleaned[:max_chars]
    cleaned = _strip_xml_boundaries(cleaned)
    cleaned = _apply_injection_filters(cleaned)
    return f"<user_query>{cleaned}</user_query>"


def sanitize_source_content(
    content: str,
    file_path: str,
    max_chars: int = _DEFAULT_MAX_SOURCE_CHARS,
) -> str:
    if not content:
        return content
    cleaned = _strip_control_chars(content)
    cleaned = cleaned[:max_chars]
    cleaned = _strip_xml_boundaries(cleaned)
    cleaned = _apply_secret_filters(cleaned)
    cleaned = _apply_injection_filters(cleaned)
    return cleaned
