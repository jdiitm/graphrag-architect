from __future__ import annotations

from typing import Any, Dict, List

import pytest

from orchestrator.app.context_manager import TokenBudget


class TestIdentifyConnectedPaths:
    def test_single_chain_identified(self) -> None:
        from orchestrator.app.context_manager import identify_connected_paths

        candidates = [
            {"id": "A", "source": "A", "target": "B", "score": 0.9},
            {"id": "B", "source": "B", "target": "C", "score": 0.7},
        ]
        paths = identify_connected_paths(candidates)
        assert len(paths) == 1
        assert len(paths[0]) == 2

    def test_isolated_nodes_become_singleton_paths(self) -> None:
        from orchestrator.app.context_manager import identify_connected_paths

        candidates = [
            {"id": "A", "score": 0.9},
            {"id": "B", "score": 0.5},
        ]
        paths = identify_connected_paths(candidates)
        assert len(paths) == 2
        for path in paths:
            assert len(path) == 1

    def test_disjoint_chains_separated(self) -> None:
        from orchestrator.app.context_manager import identify_connected_paths

        candidates = [
            {"id": "A", "source": "A", "target": "B", "score": 0.9},
            {"id": "B", "source": "B", "target": "C", "score": 0.7},
            {"id": "D", "source": "D", "target": "E", "score": 0.6},
        ]
        paths = identify_connected_paths(candidates)
        assert len(paths) == 2


class TestTopologyAwareTruncation:
    def test_isolated_nodes_dropped_before_paths(self) -> None:
        from orchestrator.app.context_manager import truncate_context_topology

        path_a = {"id": "A", "source": "A", "target": "B", "score": 0.8}
        path_b = {"id": "B", "source": "B", "target": "C", "score": 0.7}
        isolated = {"id": "X", "score": 0.3}
        candidates = [path_a, path_b, isolated]
        budget = TokenBudget(max_context_tokens=200, max_results=50)
        result = truncate_context_topology(candidates, budget)
        result_ids = {r["id"] for r in result}
        assert "A" in result_ids
        assert "B" in result_ids

    def test_connected_path_dropped_as_whole_unit(self) -> None:
        from orchestrator.app.context_manager import truncate_context_topology

        chain1 = [
            {"id": "A", "source": "A", "target": "B", "score": 0.9},
            {"id": "B", "source": "B", "target": "C", "score": 0.8},
        ]
        chain2 = [
            {"id": "D", "source": "D", "target": "E", "score": 0.3},
            {"id": "E", "source": "E", "target": "F", "score": 0.2},
        ]
        candidates = chain1 + chain2

        budget = TokenBudget(max_context_tokens=200, max_results=50)
        result = truncate_context_topology(candidates, budget)
        result_ids = {r["id"] for r in result}

        if "D" in result_ids:
            assert "E" in result_ids
        if "A" in result_ids:
            assert "B" in result_ids

    def test_paths_ranked_by_min_edge_score(self) -> None:
        from orchestrator.app.context_manager import truncate_context_topology

        high_path = [
            {"id": "H1", "source": "H1", "target": "H2", "score": 0.95},
            {"id": "H2", "source": "H2", "target": "H3", "score": 0.90},
        ]
        low_path = [
            {"id": "L1", "source": "L1", "target": "L2", "score": 0.2},
            {"id": "L2", "source": "L2", "target": "L3", "score": 0.1},
        ]
        candidates = low_path + high_path
        budget = TokenBudget(max_context_tokens=200, max_results=50)
        result = truncate_context_topology(candidates, budget)
        result_ids = {r["id"] for r in result}
        assert "H1" in result_ids
        assert "H2" in result_ids

    def test_empty_candidates(self) -> None:
        from orchestrator.app.context_manager import truncate_context_topology

        budget = TokenBudget(max_context_tokens=200, max_results=50)
        assert truncate_context_topology([], budget) == []

    def test_existing_truncate_context_unchanged(self) -> None:
        from orchestrator.app.context_manager import truncate_context

        candidates = [{"id": "A", "score": 0.9}]
        budget = TokenBudget(max_context_tokens=10000)
        result = truncate_context(candidates, budget)
        assert result == candidates
