from __future__ import annotations

from typing import Any, Dict, List

import pytest

from orchestrator.app.context_manager import (
    TokenBudget,
    compress_component_to_summaries,
    estimate_tokens,
    truncate_context_topology,
)


def _build_two_community_component() -> List[Dict[str, Any]]:
    community_a = [
        {"id": "A1", "source": "A1", "target": "A2", "score": 0.9},
        {"id": "A2", "source": "A2", "target": "A3", "score": 0.9},
        {"id": "A3", "source": "A3", "target": "A4", "score": 0.9},
        {"id": "A4", "source": "A4", "target": "A5", "score": 0.9},
        {"id": "A5", "source": "A5", "target": "A6", "score": 0.9},
    ]
    community_b = [
        {"id": "B1", "source": "B1", "target": "B2", "score": 0.8},
        {"id": "B2", "source": "B2", "target": "B3", "score": 0.8},
        {"id": "B3", "source": "B3", "target": "B4", "score": 0.8},
        {"id": "B4", "source": "B4", "target": "B5", "score": 0.8},
    ]
    bridge = [
        {"id": "bridge-AB", "source": "A3", "target": "B1", "score": 0.85},
    ]
    return community_a + bridge + community_b


def _build_single_community_component() -> List[Dict[str, Any]]:
    return [
        {"id": "X1", "source": "X1", "target": "X2", "score": 0.9},
        {"id": "X2", "source": "X2", "target": "X3", "score": 0.9},
    ]


class TestCompressComponentToSummaries:

    def test_multi_community_produces_summary_per_community(self) -> None:
        component = _build_two_community_component()
        budget = TokenBudget(max_context_tokens=200, max_results=50)
        summaries = compress_component_to_summaries(component, budget)

        assert len(summaries) >= 2
        for s in summaries:
            assert "community_id" in s
            assert "member_count" in s
            assert isinstance(s["member_count"], int)
            assert s["member_count"] > 0

    def test_summaries_include_member_names(self) -> None:
        component = _build_two_community_component()
        budget = TokenBudget(max_context_tokens=500, max_results=50)
        summaries = compress_component_to_summaries(component, budget)

        for s in summaries:
            assert "members" in s
            assert isinstance(s["members"], list)
            assert len(s["members"]) > 0

    def test_summaries_fit_within_token_budget(self) -> None:
        component = _build_two_community_component()
        budget = TokenBudget(max_context_tokens=300, max_results=50)
        summaries = compress_component_to_summaries(component, budget)

        total_tokens = sum(
            estimate_tokens(str(s)) for s in summaries
        )
        assert total_tokens <= budget.max_context_tokens

    def test_empty_component_returns_empty(self) -> None:
        budget = TokenBudget(max_context_tokens=1000, max_results=50)
        summaries = compress_component_to_summaries([], budget)
        assert summaries == []

    def test_single_community_returns_pagerank_fallback(self) -> None:
        component = _build_single_community_component()
        budget = TokenBudget(max_context_tokens=50, max_results=50)
        result = compress_component_to_summaries(component, budget)

        has_summary = any("community_id" in r for r in result)
        assert not has_summary, (
            "Single-community component should fall back to PageRank "
            "truncation, not produce community summaries"
        )

    def test_summaries_preserve_cross_community_bridges(self) -> None:
        component = _build_two_community_component()
        budget = TokenBudget(max_context_tokens=500, max_results=50)
        summaries = compress_component_to_summaries(component, budget)

        bridge_found = any(
            "bridge" in str(s.get("cross_community_edges", []))
            or s.get("cross_community_edge_count", 0) > 0
            for s in summaries
        )
        assert bridge_found, (
            "Summaries must indicate cross-community connections exist"
        )


class TestTruncateContextTopologyWithCommunities:

    def test_oversized_component_uses_community_compression(self) -> None:
        component = _build_two_community_component()
        raw_cost = sum(estimate_tokens(str(c)) for c in component)
        budget = TokenBudget(
            max_context_tokens=raw_cost // 3,
            max_results=50,
        )

        result = truncate_context_topology(component, budget)

        has_summary = any("community_id" in r for r in result)
        assert has_summary, (
            "Oversized multi-community component should produce "
            "community summaries instead of raw truncation"
        )

    def test_small_component_uses_direct_inclusion(self) -> None:
        small = _build_single_community_component()
        budget = TokenBudget(max_context_tokens=100_000, max_results=50)

        result = truncate_context_topology(small, budget)

        assert len(result) == len(small)
        has_summary = any("community_id" in r for r in result)
        assert not has_summary

    def test_mixed_components_large_gets_compressed(self) -> None:
        large = _build_two_community_component()
        small = [{"id": "iso-1", "score": 0.5}]
        candidates = large + small

        raw_cost = sum(estimate_tokens(str(c)) for c in large)
        budget = TokenBudget(
            max_context_tokens=max(raw_cost // 4, 80),
            max_results=50,
        )

        result = truncate_context_topology(candidates, budget)
        result_ids = {r.get("id", "") for r in result}
        assert "iso-1" in result_ids or any(
            "community_id" in r for r in result
        )
