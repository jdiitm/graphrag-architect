from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Set, Tuple

from orchestrator.app.graph_embeddings import GraphTopology
from orchestrator.app.prompt_sanitizer import (
    ContentFirewall,
    HMACDelimiter,
    sanitize_source_content,
)
from orchestrator.app.semantic_partitioner import SemanticPartitioner
from orchestrator.app.token_counter import count_tokens

_CONTENT_FIREWALL = ContentFirewall()
_HMAC_DELIMITER = HMACDelimiter()


@dataclass(frozen=True)
class TokenBudget:
    max_context_tokens: int = 32_000
    max_results: int = 50


@dataclass(frozen=True)
class ContextBlock:
    content: str
    delimiter: str


def estimate_tokens(text: str) -> int:
    return max(1, count_tokens(text))


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
_BRIDGE_SCORE_MULTIPLIER = 1.5


def _identify_bridge_nodes(
    adjacency: Dict[str, List[str]],
) -> Set[str]:
    if not adjacency:
        return set()

    disc: Dict[str, int] = {}
    low: Dict[str, int] = {}
    parent: Dict[str, str] = {}
    bridges: Set[str] = set()
    timer = [0]

    def _dfs(u: str) -> None:
        disc[u] = low[u] = timer[0]
        timer[0] += 1
        child_count = 0

        for v in adjacency.get(u, []):
            if v not in disc:
                child_count += 1
                parent[v] = u
                _dfs(v)
                low[u] = min(low[u], low[v])
                if parent.get(u) is None and child_count > 1:
                    bridges.add(u)
                if parent.get(u) is not None and low[v] >= disc[u]:
                    bridges.add(u)
            elif v != parent.get(u):
                low[u] = min(low[u], disc[v])

    for node in adjacency:
        if node not in disc:
            parent[node] = None  # type: ignore[assignment]
            _dfs(node)

    return bridges


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


def _score_candidates_with_bridge_boost(
    component: List[Dict[str, Any]],
    adjacency: Dict[str, List[str]],
) -> List[Tuple[float, Dict[str, Any]]]:
    pr_scores = _pagerank_scores(adjacency)
    bridge_nodes = _identify_bridge_nodes(adjacency)
    max_pr = max(pr_scores.values()) if pr_scores else 1.0
    bridge_boost = max_pr * _BRIDGE_SCORE_MULTIPLIER

    scored: List[Tuple[float, Dict[str, Any]]] = []
    for candidate in component:
        ids = _candidate_node_ids(candidate)
        score = max((pr_scores.get(nid, 0.0) for nid in ids), default=0.0)
        if any(nid in bridge_nodes for nid in ids):
            score = max(score, bridge_boost)
        scored.append((score, candidate))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored


def _truncate_component_by_pagerank(
    component: List[Dict[str, Any]],
    token_budget: int,
    max_results: int,
) -> List[Dict[str, Any]]:
    adjacency = _build_component_adjacency(component)
    scored_candidates = _score_candidates_with_bridge_boost(component, adjacency)

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


def _build_topology_from_component(
    component: List[Dict[str, Any]],
) -> GraphTopology:
    all_nodes: Set[str] = set()
    adj: Dict[str, List[str]] = defaultdict(list)
    for candidate in component:
        ids = _candidate_node_ids(candidate)
        all_nodes.update(ids)
        if len(ids) == 2:
            adj[ids[0]].append(ids[1])
            adj[ids[1]].append(ids[0])
    for node in all_nodes:
        if node not in adj:
            adj[node] = []
    return GraphTopology(nodes=list(all_nodes), adjacency=dict(adj))


def _count_cross_community_edges(
    component: List[Dict[str, Any]],
    node_to_community: Dict[str, str],
) -> int:
    count = 0
    for candidate in component:
        ids = _candidate_node_ids(candidate)
        if len(ids) == 2:
            comm_a = node_to_community.get(ids[0], "")
            comm_b = node_to_community.get(ids[1], "")
            if comm_a and comm_b and comm_a != comm_b:
                count += 1
    return count


def _collect_bridge_edges_for_community(
    component: List[Dict[str, Any]],
    community_members: Set[str],
    node_to_community: Dict[str, str],
) -> List[Dict[str, str]]:
    bridge_edges: List[Dict[str, str]] = []
    for candidate in component:
        ids = _candidate_node_ids(candidate)
        if len(ids) != 2:
            continue
        src_in = ids[0] in community_members
        tgt_in = ids[1] in community_members
        if src_in and not tgt_in:
            bridge_edges.append({
                "node": ids[0],
                "connects_to": node_to_community.get(ids[1], "unknown"),
            })
        elif tgt_in and not src_in:
            bridge_edges.append({
                "node": ids[1],
                "connects_to": node_to_community.get(ids[0], "unknown"),
            })
    return bridge_edges


def compress_component_to_summaries(
    component: List[Dict[str, Any]],
    budget: TokenBudget,
) -> List[Dict[str, Any]]:
    if not component:
        return []

    topology = _build_topology_from_component(component)
    if len(topology.nodes) < 2:
        return _truncate_component_by_pagerank(
            component, budget.max_context_tokens, budget.max_results,
        )

    partitioner = SemanticPartitioner()
    partition_result = partitioner.partition(topology)

    if partition_result.community_count <= 1:
        return _truncate_component_by_pagerank(
            component, budget.max_context_tokens, budget.max_results,
        )

    cross_edges = _count_cross_community_edges(
        component, partition_result.node_to_community,
    )

    summaries: List[Dict[str, Any]] = []
    total_tokens = 0

    for community in partition_result.communities:
        bridge_edges = _collect_bridge_edges_for_community(
            component, community.members,
            partition_result.node_to_community,
        )
        summary: Dict[str, Any] = {
            "community_id": community.community_id,
            "member_count": len(community.members),
            "members": sorted(community.members),
            "cross_community_edge_count": cross_edges,
            "score": max(
                (c.get("score", 0.0) for c in component
                 if set(_candidate_node_ids(c)) & community.members),
                default=0.0,
            ),
        }
        base_cost = estimate_tokens(_serialize_candidate(summary))
        if total_tokens + base_cost > budget.max_context_tokens:
            break
        if bridge_edges:
            enriched = {**summary, "bridge_edges": bridge_edges}
            enriched_cost = estimate_tokens(_serialize_candidate(enriched))
            if total_tokens + enriched_cost <= budget.max_context_tokens:
                summary = enriched
                base_cost = enriched_cost
        if len(summaries) >= budget.max_results:
            break
        summaries.append(summary)
        total_tokens += base_cost

    return summaries


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
            sub_budget = TokenBudget(
                max_context_tokens=remaining_budget,
                max_results=remaining_results,
            )
            compressed = compress_component_to_summaries(path, sub_budget)
            if compressed:
                result.extend(compressed)
                total_tokens += _path_token_cost(compressed)
            else:
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


def _truncate_list_value(items: List[Any], max_chars: int) -> str:
    if not items:
        return "[]"
    result_items: List[str] = []
    remaining = len(items)
    overhead = 4
    budget = max_chars - overhead

    for item in items:
        item_repr = repr(item)
        separator_cost = 2 if result_items else 0
        summary_cost = len(f", '... {remaining} more'")
        reserved = summary_cost if remaining > 1 else 0
        if budget - reserved < len(item_repr) + separator_cost:
            break
        result_items.append(item_repr)
        budget -= len(item_repr) + separator_cost
        remaining -= 1

    omitted = len(items) - len(result_items)
    if omitted > 0:
        result_items.append(f"'... {omitted} more'")

    return "[" + ", ".join(result_items) + "]"


def _truncate_dict_value(mapping: Dict[str, Any], max_chars: int) -> str:
    if not mapping:
        return "{}"
    result_pairs: List[str] = []
    remaining = len(mapping)
    overhead = 4
    budget = max_chars - overhead

    for key, val in mapping.items():
        pair_repr = f"{key!r}: {val!r}"
        separator_cost = 2 if result_pairs else 0
        summary_cost = len(f", '... {remaining} more': '...'")
        reserved = summary_cost if remaining > 1 else 0
        if budget - reserved < len(pair_repr) + separator_cost:
            break
        result_pairs.append(pair_repr)
        budget -= len(pair_repr) + separator_cost
        remaining -= 1

    omitted = len(mapping) - len(result_pairs)
    if omitted > 0:
        result_pairs.append(f"'... {omitted} more': '...'")

    return "{" + ", ".join(result_pairs) + "}"


def _truncate_string_value(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    cutoff = max_chars - 3
    if cutoff <= 0:
        return text[:max_chars] + "..."
    space_idx = text.rfind(" ", 0, cutoff + 1)
    if space_idx > cutoff // 2:
        return text[:space_idx] + "..."
    return text[:cutoff] + "..."


def _truncate_value(value: Any, max_chars: int) -> str:
    if isinstance(value, list):
        full = repr(value)
        return full if len(full) <= max_chars else _truncate_list_value(
            value, max_chars,
        )

    if isinstance(value, dict):
        full = repr(value)
        return full if len(full) <= max_chars else _truncate_dict_value(
            value, max_chars,
        )

    text = str(value)
    if len(text) <= max_chars:
        return text

    if isinstance(value, str):
        return _truncate_string_value(text, max_chars)

    return text[:max_chars] + "..."


def _generate_context_delimiter() -> str:
    return _HMAC_DELIMITER.generate()


def format_context_for_prompt(
    context: List[Dict[str, Any]],
    max_chars_per_value: int = 500,
) -> ContextBlock:
    if not context:
        return ContextBlock(content="", delimiter="")
    delimiter = _generate_context_delimiter()
    lines: List[str] = []
    for i, record in enumerate(context, 1):
        lines.append(f"[{i}]")
        for key, value in record.items():
            sanitized_key = sanitize_source_content(
                str(key), f"context_key_{i}",
            )
            truncated = _truncate_value(value, max_chars_per_value)
            firewall_cleaned = _CONTENT_FIREWALL.sanitize(truncated)
            sanitized_value = sanitize_source_content(
                firewall_cleaned, f"context_field_{key}",
            )
            lines.append(
                f"  {sanitized_key}: {sanitized_value}",
            )
    body = "\n".join(lines)
    return ContextBlock(
        content=f"<{delimiter}>{body}</{delimiter}>",
        delimiter=delimiter,
    )


_CONTEXT_BLOCK_TAG = re.compile(
    r"\A<(GRAPHCTX_[A-Za-z0-9]+_[A-Za-z0-9]+)>([\s\S]*)</\1>\Z",
)


def parse_context_block(raw: str) -> ContextBlock:
    match = _CONTEXT_BLOCK_TAG.match(raw)
    if not match:
        raise ValueError("No valid context delimiter found")
    delimiter = match.group(1)
    if not _HMAC_DELIMITER.validate(delimiter):
        raise ValueError("Context block delimiter failed HMAC validation")
    return ContextBlock(content=match.group(2), delimiter=delimiter)
