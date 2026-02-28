from __future__ import annotations

import enum
import hashlib
import hmac
import html
import re
import secrets
import unicodedata
from dataclasses import dataclass
from typing import FrozenSet, List, Set, Tuple

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

_DELIMITER_PATTERN = re.compile(
    r"GRAPHCTX_[A-Za-z0-9]+(?:_[A-Za-z0-9]+)*", re.IGNORECASE,
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


def _strip_delimiter_patterns(text: str) -> str:
    return _DELIMITER_PATTERN.sub("[CONTEXT_REF]", text)


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
    cleaned = _strip_delimiter_patterns(cleaned)
    cleaned = _apply_secret_filters(cleaned)
    cleaned = _apply_injection_filters(cleaned)
    cleaned = html.escape(cleaned, quote=False)
    return cleaned


def sanitize_ingestion_content(
    content: str,
    file_path: str,
    max_chars: int = _DEFAULT_MAX_SOURCE_CHARS,
) -> str:
    """Security-sanitize content for ingestion without HTML-escaping.

    Applies the same pipeline as sanitize_source_content (control char
    stripping, XML boundary removal, delimiter redaction, secret
    filtering, injection filtering) but omits ``html.escape()`` so that
    source-code operators (``<``, ``>``, ``&``) are preserved for LLM
    entity extraction and downstream AST parsing.
    """
    if not content:
        return content
    cleaned = _strip_control_chars(content)
    cleaned = cleaned[:max_chars]
    cleaned = _strip_xml_boundaries(cleaned)
    cleaned = _strip_delimiter_patterns(cleaned)
    cleaned = _apply_secret_filters(cleaned)
    cleaned = _apply_injection_filters(cleaned)
    return cleaned


class ThreatCategory(enum.Enum):
    INSTRUCTION_OVERRIDE = "instruction_override"
    SYSTEM_PROMPT_MARKER = "system_prompt_marker"
    ROLE_INJECTION = "role_injection"
    TAG_INJECTION = "tag_injection"


@dataclass(frozen=True)
class ScanResult:
    is_threat: bool
    categories: FrozenSet[ThreatCategory]
    matched_patterns: Tuple[str, ...]


_FIREWALL_RULES: List[
    Tuple[re.Pattern[str], ThreatCategory, str]
] = [
    (re.compile(
        r"ignore\s+(all\s+)?(previous|above|prior)"
        r"\s+(instructions?|rules?|prompts?)",
        re.IGNORECASE,
    ), ThreatCategory.INSTRUCTION_OVERRIDE, "[BLOCKED]"),
    (re.compile(
        r"disregard\s+(all\s+)?"
        r"(previous|above|prior)?\s*instructions?",
        re.IGNORECASE,
    ), ThreatCategory.INSTRUCTION_OVERRIDE, "[BLOCKED]"),
    (re.compile(
        r"you\s+are\s+now\b", re.IGNORECASE,
    ), ThreatCategory.INSTRUCTION_OVERRIDE, "[BLOCKED]"),
    (re.compile(
        r"new\s+instructions?\s*:", re.IGNORECASE,
    ), ThreatCategory.INSTRUCTION_OVERRIDE, "[BLOCKED]"),
    (re.compile(
        r"forget\s+(all\s+)?(your\s+)?instructions?",
        re.IGNORECASE,
    ), ThreatCategory.INSTRUCTION_OVERRIDE, "[BLOCKED]"),

    (re.compile(
        r"<\|im_start\|>",
    ), ThreatCategory.SYSTEM_PROMPT_MARKER, ""),
    (re.compile(
        r"<\|im_end\|>",
    ), ThreatCategory.SYSTEM_PROMPT_MARKER, ""),
    (re.compile(
        r"\[/?INST\]",
    ), ThreatCategory.SYSTEM_PROMPT_MARKER, ""),
    (re.compile(
        r"<{2}/?SYS>{2}",
    ), ThreatCategory.SYSTEM_PROMPT_MARKER, ""),
    (re.compile(
        r"^\s*system\s*:(?=[ \t]+[^ \t\n]+[ \t]+[^ \t\n]+[ \t]+[^ \t\n])",
        re.IGNORECASE | re.MULTILINE,
    ), ThreatCategory.SYSTEM_PROMPT_MARKER, "[BLOCKED]"),
    (re.compile(
        r"^#{1,3}\s+System\s*$",
        re.IGNORECASE | re.MULTILINE,
    ), ThreatCategory.SYSTEM_PROMPT_MARKER, "[BLOCKED]"),

    (re.compile(
        r"^\s*assistant\s*:(?=[ \t]+[^ \t\n]+[ \t]+[^ \t\n]+[ \t]+[^ \t\n])",
        re.IGNORECASE | re.MULTILINE,
    ), ThreatCategory.ROLE_INJECTION, "[BLOCKED]"),
    (re.compile(
        r"^\s*user\s*:(?=[ \t]+[^ \t\n]+[ \t]+[^ \t\n]+[ \t]+[^ \t\n])",
        re.IGNORECASE | re.MULTILINE,
    ), ThreatCategory.ROLE_INJECTION, "[BLOCKED]"),
    (re.compile(
        r"^\s*Human\s*:(?=[ \t]+[^ \t\n]+[ \t]+[^ \t\n]+[ \t]+[^ \t\n])",
        re.IGNORECASE | re.MULTILINE,
    ), ThreatCategory.ROLE_INJECTION, "[BLOCKED]"),
    (re.compile(
        r"^\s*AI\s*:(?=[ \t]+[^ \t\n]+[ \t]+[^ \t\n]+[ \t]+[^ \t\n])",
        re.IGNORECASE | re.MULTILINE,
    ), ThreatCategory.ROLE_INJECTION, "[BLOCKED]"),

    (re.compile(
        r"<\s*/?\s*"
        r"(?:graph_context|user_query|system|assistant)"
        r"\s*>",
        re.IGNORECASE,
    ), ThreatCategory.TAG_INJECTION, ""),
]


class ContentFirewall:
    _patterns: List[
        Tuple[re.Pattern[str], ThreatCategory, str]
    ]

    def __init__(self) -> None:
        self._patterns = _FIREWALL_RULES

    def scan(self, content: str) -> ScanResult:
        detected_categories: Set[ThreatCategory] = set()
        detected_patterns: List[str] = []
        for pattern, category, _ in self._patterns:
            if pattern.search(content):
                detected_categories.add(category)
                detected_patterns.append(pattern.pattern)
        return ScanResult(
            is_threat=bool(detected_categories),
            categories=frozenset(detected_categories),
            matched_patterns=tuple(detected_patterns),
        )

    def sanitize(self, content: str) -> str:
        result = content
        for pattern, _, replacement in self._patterns:
            result = pattern.sub(replacement, result)
        return result


_CLASSIFIER_RULES: List[
    Tuple[re.Pattern[str], str, float]
] = [
    (re.compile(
        r"ignore\s+(all\s+)?(previous|above|prior)"
        r"\s+(instructions?|rules?|prompts?)",
        re.IGNORECASE,
    ), "instruction_override", 0.4),
    (re.compile(
        r"disregard\s+(all\s+)?(previous|above|prior)?\s*instructions?",
        re.IGNORECASE,
    ), "instruction_override", 0.4),
    (re.compile(
        r"forget\s+(all\s+)?(your\s+)?instructions?",
        re.IGNORECASE,
    ), "instruction_override", 0.4),
    (re.compile(
        r"new\s+instructions?\s*:",
        re.IGNORECASE,
    ), "instruction_override", 0.4),

    (re.compile(
        r"you\s+are\s+now\b",
        re.IGNORECASE,
    ), "role_play", 0.35),
    (re.compile(
        r"act\s+as\s+(if\s+)?you\s+(are|were)\s+",
        re.IGNORECASE,
    ), "role_play", 0.35),
    (re.compile(
        r"pretend\s+(that\s+)?you\s+(are|were)\s+",
        re.IGNORECASE,
    ), "role_play", 0.35),

    (re.compile(
        r"^\s*SYSTEM\s*:",
        re.IGNORECASE | re.MULTILINE,
    ), "system_mimicry", 0.35),
    (re.compile(
        r"\[SYSTEM\]",
        re.IGNORECASE,
    ), "system_mimicry", 0.35),
    (re.compile(
        r"^#{1,3}\s+System\s*(Message)?\s*$",
        re.IGNORECASE | re.MULTILINE,
    ), "system_mimicry", 0.35),
    (re.compile(
        r"<\|im_start\|>",
    ), "system_mimicry", 0.4),
    (re.compile(
        r"<\|im_end\|>",
    ), "system_mimicry", 0.4),
    (re.compile(
        r"\[/?INST\]",
    ), "system_mimicry", 0.35),
    (re.compile(
        r"<{2}/?SYS>{2}",
    ), "system_mimicry", 0.4),

    (re.compile(
        r"[A-Za-z0-9+/]{40,}={0,2}",
    ), "encoding_obfuscation", 0.35),
    (re.compile(
        r"(\\x[0-9a-fA-F]{2}){4,}",
    ), "encoding_obfuscation", 0.35),
    (re.compile(
        r"(\\u[0-9a-fA-F]{4}){4,}",
    ), "encoding_obfuscation", 0.35),

    (re.compile(
        r"<\s*/?\s*(?:graph_context|user_query|system|assistant)\s*>",
        re.IGNORECASE,
    ), "delimiter_escape", 0.4),
    (re.compile(
        r"GRAPHCTX_[A-Za-z0-9]+(?:_[A-Za-z0-9]+)*",
        re.IGNORECASE,
    ), "delimiter_escape", 0.4),
]


@dataclass(frozen=True)
class InjectionResult:
    score: float
    detected_patterns: List[str]
    is_flagged: bool


class PromptInjectionClassifier:
    _threshold: float
    _patterns: List[Tuple[re.Pattern[str], str, float]]

    def __init__(self, threshold: float = 0.3) -> None:
        self._threshold = threshold
        self._patterns = _CLASSIFIER_RULES

    def classify(self, text: str) -> InjectionResult:
        text = unicodedata.normalize("NFKC", text)
        total_score = 0.0
        detected: List[str] = []
        for pattern, name, weight in self._patterns:
            if pattern.search(text):
                total_score += weight
                detected.append(name)
        capped_score = min(total_score, 1.0)
        return InjectionResult(
            score=capped_score,
            detected_patterns=detected,
            is_flagged=capped_score > self._threshold,
        )

    def strip_flagged_content(
        self, text: str, result: InjectionResult,
    ) -> str:
        if not result.is_flagged:
            return text
        matched_categories = set(result.detected_patterns)
        cleaned = text
        for pattern, name, _ in self._patterns:
            if name in matched_categories:
                cleaned = pattern.sub("[BLOCKED]", cleaned)
        return cleaned


class HMACDelimiter:
    _key: bytes

    def __init__(self) -> None:
        self._key = secrets.token_bytes(32)

    def generate(self) -> str:
        nonce = secrets.token_hex(12)
        signature = hmac.new(
            self._key, nonce.encode(), hashlib.sha256,
        ).hexdigest()[:16]
        return f"GRAPHCTX_{nonce}_{signature}"

    def validate(self, delimiter: str) -> bool:
        parts = delimiter.split("_", 2)
        if len(parts) != 3 or parts[0] != "GRAPHCTX":
            return False
        nonce, candidate_sig = parts[1], parts[2]
        expected_sig = hmac.new(
            self._key, nonce.encode(), hashlib.sha256,
        ).hexdigest()[:16]
        return hmac.compare_digest(
            candidate_sig, expected_sig,
        )
