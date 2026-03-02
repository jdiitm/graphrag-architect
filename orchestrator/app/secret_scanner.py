from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True)
class SecretPattern:
    name: str
    pattern: str
    severity: str


@dataclass(frozen=True)
class SecretFinding:
    pattern_name: str
    line: int
    column: int
    matched_text_preview: str


_PREVIEW_MAX_LENGTH = 12

PATTERNS: List[SecretPattern] = [
    SecretPattern(
        name="aws_access_key",
        pattern=r"AKIA[A-Z0-9]{16}",
        severity="critical",
    ),
    SecretPattern(
        name="github_token",
        pattern=r"gh[ps]_[A-Za-z0-9]{36,}",
        severity="critical",
    ),
    SecretPattern(
        name="github_pat",
        pattern=r"github_pat_[A-Za-z0-9_]{30,}",
        severity="critical",
    ),
    SecretPattern(
        name="openai_key",
        pattern=r"sk-(?:proj|live)-[A-Za-z0-9]{20,}",
        severity="critical",
    ),
    SecretPattern(
        name="private_key",
        pattern=(
            r"-----BEGIN\s+(?:RSA|EC|DSA|OPENSSH|PGP)?\s*PRIVATE\s+KEY-----"
            r"[\s\S]*?"
            r"-----END\s+(?:RSA|EC|DSA|OPENSSH|PGP)?\s*PRIVATE\s+KEY-----"
        ),
        severity="critical",
    ),
    SecretPattern(
        name="password_assignment",
        pattern=(
            r"(?i)(?:^|(?<=\s))[A-Z_]*password[A-Z_]*"
            r"\s*[=:]\s*[\"'][^\s\"']{4,}[\"']"
        ),
        severity="high",
    ),
    SecretPattern(
        name="generic_api_key",
        pattern=(
            r"(?i)(?:api_key|apikey|api-key)"
            r"\s*[=:]\s*[\"'][A-Za-z0-9+/=_\-]{16,}[\"']"
        ),
        severity="high",
    ),
    SecretPattern(
        name="jwt_token",
        pattern=(
            r"eyJ[A-Za-z0-9_-]{10,}"
            r"\.eyJ[A-Za-z0-9_-]{10,}"
            r"\.[A-Za-z0-9_-]{10,}"
        ),
        severity="high",
    ),
]

_COMPILED_PATTERNS: List[Tuple[SecretPattern, re.Pattern[str]]] = [
    (sp, re.compile(sp.pattern, re.MULTILINE)) for sp in PATTERNS
]


def _truncate_preview(matched_text: str) -> str:
    if len(matched_text) <= _PREVIEW_MAX_LENGTH:
        return matched_text
    return matched_text[:_PREVIEW_MAX_LENGTH - 4] + "****"


def _line_and_column(content: str, match_start: int) -> Tuple[int, int]:
    line = content[:match_start].count("\n") + 1
    last_newline = content.rfind("\n", 0, match_start)
    column = match_start - last_newline if last_newline >= 0 else match_start + 1
    return line, column


def scan_content(content: str) -> List[SecretFinding]:
    if not content:
        return []
    findings: List[SecretFinding] = []
    for secret_pattern, compiled in _COMPILED_PATTERNS:
        for match in compiled.finditer(content):
            line, column = _line_and_column(content, match.start())
            preview = _truncate_preview(match.group(0))
            findings.append(SecretFinding(
                pattern_name=secret_pattern.name,
                line=line,
                column=column,
                matched_text_preview=preview,
            ))
    return findings


def redact_content(content: str) -> str:
    if not content:
        return content
    result = content
    for secret_pattern, compiled in _COMPILED_PATTERNS:
        replacement = f"[REDACTED-{secret_pattern.name}]"
        result = compiled.sub(replacement, result)
    return result


def scan_and_redact(content: str) -> Tuple[str, List[SecretFinding]]:
    if not content:
        return content, []
    findings = scan_content(content)
    redacted = redact_content(content)
    return redacted, findings
