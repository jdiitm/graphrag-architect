from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass(frozen=True)
class TokenBudget:
    max_context_tokens: int = 32_000
    max_results: int = 50


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def _serialize_candidate(candidate: Any) -> str:
    if isinstance(candidate, dict):
        return json.dumps(candidate, default=str)
    return str(candidate)


def truncate_context(
    candidates: List[Dict[str, Any]],
    budget: TokenBudget,
) -> List[Dict[str, Any]]:
    if not candidates:
        return []

    capped = candidates[:budget.max_results]

    result: List[Dict[str, Any]] = []
    total_tokens = 0
    for candidate in capped:
        candidate_tokens = estimate_tokens(_serialize_candidate(candidate))
        if total_tokens + candidate_tokens > budget.max_context_tokens:
            break
        result.append(candidate)
        total_tokens += candidate_tokens
    return result
