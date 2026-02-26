from __future__ import annotations

from typing import Any, Dict, List, Set

import pytest

from orchestrator.app.context_manager import (
    TokenBudget,
    _build_component_adjacency,
    _identify_bridge_nodes,
    _truncate_component_by_pagerank,
    compress_component_to_summaries,
    estimate_tokens,
    truncate_context_topology,
)


def _make_edge(
    source: str, target: str, score: float = 0.5,
) -> Dict[str, Any]:
    return {"id": f"{source}-{target}", "source": source, "target": target, "score": score}


class TestIdentifyBridgeNodes:

    def test_chain_interior_nodes_are_bridges(self) -> None:
        adj: Dict[str, List[str]] = {
            "A": ["B"], "B": ["A", "C"], "C": ["B", "D"], "D": ["C"],
        }
        bridges = _identify_bridge_nodes(adj)
        assert "B" in bridges
        assert "C" in bridges
        assert "A" not in bridges
        assert "D" not in bridges

    def test_star_center_is_bridge(self) -> None:
        adj: Dict[str, List[str]] = {
            "center": ["A", "B", "C"],
            "A": ["center"],
            "B": ["center"],
            "C": ["center"],
        }
        bridges = _identify_bridge_nodes(adj)
        assert bridges == {"center"}

    def test_diamond_graph_no_bridges(self) -> None:
        adj: Dict[str, List[str]] = {
            "A": ["B", "C"], "B": ["A", "D"],
            "C": ["A", "D"], "D": ["B", "C"],
        }
        bridges = _identify_bridge_nodes(adj)
        assert bridges == set()

    def test_complete_graph_no_bridges(self) -> None:
        nodes = ["A", "B", "C", "D"]
        adj: Dict[str, List[str]] = {
            n: [m for m in nodes if m != n] for n in nodes
        }
        bridges = _identify_bridge_nodes(adj)
        assert bridges == set()

    def test_single_bridge_between_clusters(self) -> None:
        adj: Dict[str, List[str]] = {
            "A1": ["A2", "A3"],
            "A2": ["A1", "A3", "bridge"],
            "A3": ["A1", "A2"],
            "bridge": ["A2", "B1"],
            "B1": ["bridge", "B2", "B3"],
            "B2": ["B1", "B3"],
            "B3": ["B1", "B2"],
        }
        bridges = _identify_bridge_nodes(adj)
        assert "bridge" in bridges

    def test_empty_graph(self) -> None:
        bridges = _identify_bridge_nodes({})
        assert bridges == set()

    def test_single_node(self) -> None:
        bridges = _identify_bridge_nodes({"A": []})
        assert bridges == set()


class TestBridgeAwareTruncation:

    def _build_bridged_component(self) -> List[Dict[str, Any]]:
        cluster_a = [
            _make_edge("A1", "A2", 0.9),
            _make_edge("A2", "A3", 0.9),
            _make_edge("A1", "A3", 0.9),
        ]
        bridge_edge = [_make_edge("A2", "B1", 0.3)]
        cluster_b = [
            _make_edge("B1", "B2", 0.8),
            _make_edge("B2", "B3", 0.8),
            _make_edge("B1", "B3", 0.8),
        ]
        return cluster_a + bridge_edge + cluster_b

    def test_bridge_node_survives_tight_budget(self) -> None:
        component = self._build_bridged_component()
        raw_cost = sum(estimate_tokens(str(c)) for c in component)
        tight_budget = raw_cost // 3

        result = _truncate_component_by_pagerank(
            component, tight_budget, 50,
        )

        result_nodes: Set[str] = set()
        for c in result:
            src = c.get("source", "")
            tgt = c.get("target", "")
            if src:
                result_nodes.add(src)
            if tgt:
                result_nodes.add(tgt)

        assert "A2" in result_nodes or "B1" in result_nodes, (
            "Bridge node (A2 or B1) must survive truncation"
        )

    def test_regression_no_bridges_behaves_like_vanilla_pagerank(self) -> None:
        component = [
            _make_edge("A", "B", 0.9),
            _make_edge("B", "C", 0.9),
            _make_edge("C", "A", 0.9),
        ]
        result = _truncate_component_by_pagerank(
            component, 100_000, 50,
        )
        assert len(result) == len(component)


class TestEnhancedCommunitySummaries:

    def _build_two_cluster_with_bridge(self) -> List[Dict[str, Any]]:
        cluster_a = [
            _make_edge("A1", "A2", 0.9),
            _make_edge("A2", "A3", 0.9),
            _make_edge("A3", "A4", 0.9),
            _make_edge("A4", "A5", 0.9),
            _make_edge("A5", "A1", 0.9),
        ]
        bridge_edge = [_make_edge("A3", "B1", 0.5)]
        cluster_b = [
            _make_edge("B1", "B2", 0.8),
            _make_edge("B2", "B3", 0.8),
            _make_edge("B3", "B4", 0.8),
            _make_edge("B4", "B1", 0.8),
        ]
        return cluster_a + bridge_edge + cluster_b

    def test_summaries_include_per_community_bridge_edges(self) -> None:
        component = self._build_two_cluster_with_bridge()
        budget = TokenBudget(max_context_tokens=2000, max_results=50)
        summaries = compress_component_to_summaries(component, budget)

        has_bridge_details = any(
            "bridge_edges" in s for s in summaries
        )
        assert has_bridge_details, (
            "Community summaries must include bridge_edges details"
        )

    def test_bridge_edges_list_connected_communities(self) -> None:
        component = self._build_two_cluster_with_bridge()
        budget = TokenBudget(max_context_tokens=2000, max_results=50)
        summaries = compress_component_to_summaries(component, budget)

        all_bridge_edges = []
        for s in summaries:
            all_bridge_edges.extend(s.get("bridge_edges", []))

        assert len(all_bridge_edges) > 0, (
            "Must have at least one bridge edge in summaries"
        )
        for be in all_bridge_edges:
            assert "node" in be
            assert "connects_to" in be


class TestTopologyTruncationBridgePreservation:

    def test_oversized_component_preserves_bridge_info(self) -> None:
        cluster_a = [
            _make_edge("A1", "A2", 0.9),
            _make_edge("A2", "A3", 0.9),
            _make_edge("A3", "A4", 0.9),
            _make_edge("A4", "A5", 0.9),
            _make_edge("A5", "A1", 0.9),
        ]
        bridge = [_make_edge("A3", "B1", 0.4)]
        cluster_b = [
            _make_edge("B1", "B2", 0.8),
            _make_edge("B2", "B3", 0.8),
            _make_edge("B3", "B4", 0.8),
            _make_edge("B4", "B1", 0.8),
        ]
        component = cluster_a + bridge + cluster_b

        raw_cost = sum(estimate_tokens(str(c)) for c in component)
        budget = TokenBudget(
            max_context_tokens=raw_cost // 3,
            max_results=50,
        )

        result = truncate_context_topology(component, budget)

        has_bridge = any(
            "bridge_edges" in r
            or r.get("cross_community_edge_count", 0) > 0
            or r.get("source") in ("A3", "B1")
            or r.get("target") in ("A3", "B1")
            for r in result
        )
        assert has_bridge, (
            "Truncated topology must preserve bridge node or bridge info"
        )
