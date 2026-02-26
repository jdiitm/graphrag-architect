from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Set, Tuple

from orchestrator.app.prompt_sanitizer import sanitize_source_content


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


_PAGERANK_ITERATIONS = 10
_PAGERANK_DAMPING = 0.85


def _pagerank_scores(
    adjacency: Dict[str, List[str]],
    iterations: int = _PAGERANK_ITERATIONS,
    damping: float = _PAGERANK_DAMPING,
) -> Dict[str, float]:
    if not adjacency:
        return {}
    nodes = list(adjacency.keys())
    n = len(nodes)
    scores: Dict[str, float] = {node: 1.0 / n for node in nodes}
    for _ in range(iterations):
        new_scores: Dict[str, float] = {}
        for node in nodes:
            rank = (1.0 - damping) / n
            for src in nodes:
                neighbors = adjacency.get(src, [])
                if node in neighbors and len(neighbors) > 0:
                    rank += damping * scores[src] / len(neighbors)
            new_scores[node] = rank
        scores = new_scores
    return scores


def _build_component_adjacency(
    component: List[Dict[str, Any]],
) -> Dict[str, List[str]]:
    adj: Dict[str, Set[str]] = defaultdict(set)
    all_nodes: Set[str] = set()
    for candidate in component:
        ids = _candidate_node_ids(candidate)
        all_nodes.update(ids)
        if len(ids) == 2:
            adj[ids[0]].add(ids[1])
            adj[ids[1]].add(ids[0])
    for node in all_nodes:
        if node not in adj:
            adj[node] = set()
    return {k: list(v) for k, v in adj.items()}


def _truncate_component_by_pagerank(
    component: List[Dict[str, Any]],
    token_budget: int,
    max_results: int,
) -> List[Dict[str, Any]]:
    adjacency = _build_component_adjacency(component)
    pr_scores = _pagerank_scores(adjacency)

    scored_candidates = []
    for c in component:
        ids = _candidate_node_ids(c)
        c_score = max((pr_scores.get(nid, 0.0) for nid in ids), default=0.0)
        scored_candidates.append((c_score, c))
    scored_candidates.sort(key=lambda x: x[0], reverse=True)

    result: List[Dict[str, Any]] = []
    included_nodes: Set[str] = set()
    total_tokens = 0

    for _, candidate in scored_candidates:
        cost = estimate_tokens(_serialize_candidate(candidate))
        if total_tokens + cost > token_budget:
            continue
        if len(result) >= max_results:
            break

        ids = _candidate_node_ids(candidate)
        if result and len(ids) == 2:
            if ids[0] not in included_nodes and ids[1] not in included_nodes:
                continue

        result.append(candidate)
        total_tokens += cost
        included_nodes.update(ids)

    return result


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
        remaining_budget = budget.max_context_tokens - total_tokens
        remaining_results = budget.max_results - len(result)
        if remaining_results <= 0:
            break
        if cost <= remaining_budget and len(path) <= remaining_results:
            result.extend(path)
            total_tokens += cost
        elif remaining_budget > 0:
            partial = _truncate_component_by_pagerank(
                path, remaining_budget, remaining_results,
            )
            result.extend(partial)
            total_tokens += _path_token_cost(partial)

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
            truncated = _truncate_value(value, max_chars_per_value)
            sanitized = sanitize_source_content(truncated, f"context_field_{key}")
            lines.append(f"  {key}: {sanitized}")
    body = "\n".join(lines)
    return f"<graph_context>{body}</graph_context>"
