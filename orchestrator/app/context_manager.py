from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Set, Tuple


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


def _rank_by_relevance(
    candidates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    has_scores = any("score" in c for c in candidates)
    if not has_scores:
        return candidates
    return sorted(candidates, key=lambda c: c.get("score", 0.0), reverse=True)


def truncate_context(
    candidates: List[Dict[str, Any]],
    budget: TokenBudget,
) -> List[Dict[str, Any]]:
    if not candidates:
        return []

    ranked = _rank_by_relevance(candidates)
    capped = ranked[:budget.max_results]

    result: List[Dict[str, Any]] = []
    total_tokens = 0
    for candidate in capped:
        candidate_tokens = estimate_tokens(_serialize_candidate(candidate))
        if total_tokens + candidate_tokens > budget.max_context_tokens:
            break
        result.append(candidate)
        total_tokens += candidate_tokens
    return result


def _candidate_node_ids(candidate: Dict[str, Any]) -> Tuple[str, ...]:
    ids: List[str] = []
    for key in ("source", "target"):
        val = candidate.get(key)
        if isinstance(val, str) and val:
            ids.append(val)
    if not ids:
        cid = candidate.get("id")
        if isinstance(cid, str) and cid:
            ids.append(cid)
    return tuple(ids)


def identify_connected_paths(
    candidates: List[Dict[str, Any]],
) -> List[List[Dict[str, Any]]]:
    if not candidates:
        return []

    adjacency: Dict[str, Set[int]] = defaultdict(set)
    for idx, c in enumerate(candidates):
        node_ids = _candidate_node_ids(c)
        for nid in node_ids:
            adjacency[nid].add(idx)

    visited: Set[int] = set()
    components: List[List[Dict[str, Any]]] = []
    for idx in range(len(candidates)):
        if idx in visited:
            continue
        component_indices: List[int] = []
        stack = [idx]
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            component_indices.append(current)
            for nid in _candidate_node_ids(candidates[current]):
                for neighbor_idx in adjacency[nid]:
                    if neighbor_idx not in visited:
                        stack.append(neighbor_idx)
        components.append([candidates[i] for i in sorted(component_indices)])
    return components


def _path_min_score(path: List[Dict[str, Any]]) -> float:
    scores = [c.get("score", 0.0) for c in path]
    return min(scores) if scores else 0.0


def _path_token_cost(path: List[Dict[str, Any]]) -> int:
    return sum(estimate_tokens(_serialize_candidate(c)) for c in path)


def truncate_context_topology(
    candidates: List[Dict[str, Any]],
    budget: TokenBudget,
) -> List[Dict[str, Any]]:
    if not candidates:
        return []

    paths = identify_connected_paths(candidates)

    isolated = [p for p in paths if len(p) == 1]
    connected = [p for p in paths if len(p) > 1]

    connected.sort(key=_path_min_score, reverse=True)
    isolated.sort(key=lambda p: p[0].get("score", 0.0), reverse=True)

    result: List[Dict[str, Any]] = []
    total_tokens = 0

    for path in connected:
        cost = _path_token_cost(path)
        if total_tokens + cost > budget.max_context_tokens:
            continue
        if len(result) + len(path) > budget.max_results:
            continue
        result.extend(path)
        total_tokens += cost

    for path in isolated:
        cost = _path_token_cost(path)
        if total_tokens + cost > budget.max_context_tokens:
            break
        if len(result) >= budget.max_results:
            break
        result.extend(path)
        total_tokens += cost

    return result


def _truncate_value(value: Any, max_chars: int) -> str:
    text = str(value)
    if len(text) > max_chars:
        return text[:max_chars] + "..."
    return text


def format_context_for_prompt(
    context: List[Dict[str, Any]],
    max_chars_per_value: int = 500,
) -> str:
    if not context:
        return ""
    lines: List[str] = []
    for i, record in enumerate(context, 1):
        lines.append(f"[{i}]")
        for key, value in record.items():
            formatted = _truncate_value(value, max_chars_per_value)
            lines.append(f"  {key}: {formatted}")
    return "\n".join(lines)
