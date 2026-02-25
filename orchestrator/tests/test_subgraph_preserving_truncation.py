from __future__ import annotations

from typing import Any, Dict, List

import pytest

from orchestrator.app.context_manager import (
    TokenBudget,
    _candidate_node_ids,
    estimate_tokens,
    truncate_context_topology,
)


def _build_chain(prefix: str, length: int, score: float) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for i in range(length):
        src = f"{prefix}{i}"
        tgt = f"{prefix}{i + 1}"
        candidates.append({"id": src, "source": src, "target": tgt, "score": score})
    return candidates


def _build_star(hub: str, spokes: int, score: float) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for i in range(spokes):
        leaf = f"leaf-{i}"
        candidates.append({
            "id": f"edge-{hub}-{leaf}",
            "source": hub,
            "target": leaf,
            "score": score,
        })
    return candidates


class TestSubgraphPreservingTruncation:

    def test_oversized_component_partially_included(self) -> None:
        big_chain = _build_chain("N", 20, score=0.9)
        total_cost = sum(
            estimate_tokens(str(c)) for c in big_chain
        )
        budget = TokenBudget(
            max_context_tokens=total_cost // 2,
            max_results=50,
        )

        result = truncate_context_topology(big_chain, budget)

        assert 0 < len(result) < len(big_chain)

    def test_partial_component_preserves_spanning_tree(self) -> None:
        big_chain = _build_chain("N", 20, score=0.9)
        total_cost = sum(
            estimate_tokens(str(c)) for c in big_chain
        )
        budget = TokenBudget(
            max_context_tokens=total_cost // 2,
            max_results=50,
        )

        result = truncate_context_topology(big_chain, budget)
        all_node_ids: set = set()
        edge_node_ids: set = set()
        for r in result:
            ids = _candidate_node_ids(r)
            all_node_ids.update(ids)
            if len(ids) == 2:
                edge_node_ids.update(ids)

        for nid in edge_node_ids:
            has_connection = any(
                nid in _candidate_node_ids(c)
                for c in result
                if len(_candidate_node_ids(c)) == 2
            )
            assert has_connection, f"Node {nid} is dangling in truncated output"

    def test_hub_node_retained_over_leaves(self) -> None:
        star = _build_star("hub", spokes=15, score=0.8)
        total_cost = sum(estimate_tokens(str(c)) for c in star)
        budget = TokenBudget(
            max_context_tokens=total_cost // 3,
            max_results=50,
        )

        result = truncate_context_topology(star, budget)
        node_ids = set()
        for r in result:
            node_ids.update(_candidate_node_ids(r))

        assert "hub" in node_ids

    def test_small_component_fits_unchanged(self) -> None:
        small_chain = _build_chain("S", 3, score=0.9)
        budget = TokenBudget(max_context_tokens=100_000, max_results=50)

        result = truncate_context_topology(small_chain, budget)
        assert len(result) == len(small_chain)

    def test_mixed_components_large_partially_truncated(self) -> None:
        big = _build_chain("BIG", 20, score=0.9)
        small = _build_chain("SML", 2, score=0.5)
        isolated = [{"id": "ISO", "score": 0.3}]
        candidates = big + small + isolated

        big_cost = sum(estimate_tokens(str(c)) for c in big)
        budget = TokenBudget(
            max_context_tokens=big_cost // 2 + 500,
            max_results=50,
        )

        result = truncate_context_topology(candidates, budget)
        result_ids = {r.get("id", "") for r in result}

        has_big_nodes = any(r_id.startswith("BIG") for r_id in result_ids)
        assert has_big_nodes

    def test_empty_candidates_returns_empty(self) -> None:
        budget = TokenBudget(max_context_tokens=10_000, max_results=50)
        assert truncate_context_topology([], budget) == []


class TestPageRank:

    def test_pagerank_hub_scores_higher(self) -> None:
        from orchestrator.app.context_manager import _pagerank_scores

        adjacency = {
            "hub": ["a", "b", "c", "d"],
            "a": ["hub"],
            "b": ["hub"],
            "c": ["hub"],
            "d": ["hub"],
        }
        scores = _pagerank_scores(adjacency)

        assert scores["hub"] > scores["a"]
        assert scores["hub"] > scores["b"]

    def test_pagerank_empty_graph(self) -> None:
        from orchestrator.app.context_manager import _pagerank_scores

        scores = _pagerank_scores({})
        assert scores == {}

    def test_pagerank_single_node(self) -> None:
        from orchestrator.app.context_manager import _pagerank_scores

        scores = _pagerank_scores({"A": []})
        assert "A" in scores
        assert scores["A"] > 0.0
