from __future__ import annotations

import pytest

from orchestrator.app.context_manager import TokenBudget, truncate_context_topology


class TestTopologyAwareTruncation:

    def test_high_centrality_nodes_preserved_under_budget(self) -> None:
        candidates = [
            {"source": "peripheral-1", "target": "peripheral-2", "score": 0.9, "pagerank": 0.01},
            {"source": "auth-hub", "target": "db", "score": 0.5, "pagerank": 0.85},
            {"source": "peripheral-3", "target": "peripheral-4", "score": 0.8, "pagerank": 0.02},
        ]
        budget = TokenBudget(max_context_tokens=200, max_results=2)
        result = truncate_context_topology(candidates, budget)

        result_sources = {r.get("source") for r in result}
        assert "auth-hub" in result_sources, (
            "High-centrality node 'auth-hub' (pagerank=0.85) must be preserved "
            "over low-centrality nodes regardless of similarity score"
        )

    def test_pagerank_field_used_when_present(self) -> None:
        candidates = [
            {"source": "low-pr", "target": "x", "score": 0.95, "pagerank": 0.01},
            {"source": "high-pr", "target": "y", "score": 0.5, "pagerank": 0.9},
        ]
        budget = TokenBudget(max_context_tokens=100, max_results=1)
        result = truncate_context_topology(candidates, budget)
        assert any(r.get("source") == "high-pr" for r in result)

    def test_falls_back_to_score_when_no_pagerank(self) -> None:
        candidates = [
            {"source": "a", "target": "b", "score": 0.9},
            {"source": "c", "target": "d", "score": 0.3},
        ]
        budget = TokenBudget(max_context_tokens=100, max_results=1)
        result = truncate_context_topology(candidates, budget)
        assert any(r.get("source") == "a" for r in result)
