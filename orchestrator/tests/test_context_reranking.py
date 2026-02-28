from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest

from orchestrator.app.agentic_traversal import (
    TraversalAgent,
    TraversalState,
    TraversalStep,
)
from orchestrator.app.context_manager import TokenBudget, estimate_tokens


def _make_result(target_id: str, pagerank: float, degree: int = 1) -> Dict[str, Any]:
    return {"target_id": target_id, "pagerank": pagerank, "degree": degree}


def _token_cost(result: Dict[str, Any]) -> int:
    return estimate_tokens(str(result))


class TestHighPagerankLateSurvival:
    def test_high_pagerank_late_node_survives_truncation(self) -> None:
        sample = _make_result("x", 0.01)
        per_result = _token_cost(sample)
        tight_budget = per_result * 5 + per_result - 1

        budget = TokenBudget(max_context_tokens=tight_budget, max_results=50)
        agent = TraversalAgent(token_budget=budget)
        state = agent.create_state("start")

        for hop in range(1, 4):
            results = [_make_result(f"low-{hop}-{i}", 0.01) for i in range(3)]
            step = TraversalStep(
                node_id=f"source-{hop}",
                hop_number=hop,
                results=results,
                new_frontier=[f"source-{hop + 1}"],
            )
            agent.record_step(state, step)

        high_result = _make_result("high-value-node", 0.95, degree=50)
        step = TraversalStep(
            node_id="source-4",
            hop_number=4,
            results=[high_result],
            new_frontier=[],
        )
        agent.record_step(state, step)

        context = agent.get_context(state)
        context_ids = {item.get("target_id") for item in context}
        assert "high-value-node" in context_ids


class TestTokenBudgetRespected:
    def test_budget_not_exceeded_after_reranking(self) -> None:
        budget = TokenBudget(max_context_tokens=200, max_results=50)
        agent = TraversalAgent(token_budget=budget)
        state = agent.create_state("start")

        for hop in range(1, 6):
            results = [
                _make_result(f"node-{hop}-{i}", 0.5, degree=5) for i in range(5)
            ]
            step = TraversalStep(
                node_id=f"source-{hop}",
                hop_number=hop,
                results=results,
                new_frontier=[f"source-{hop + 1}"],
            )
            agent.record_step(state, step)

        context = agent.get_context(state)
        total_tokens = sum(
            estimate_tokens(json.dumps(item, default=str)) for item in context
        )
        assert total_tokens <= budget.max_context_tokens
        assert len(context) <= budget.max_results


class TestAllCandidatesReachTruncation:
    def test_record_step_preserves_all_candidates(self) -> None:
        sample = _make_result("x", 0.1)
        per_result = _token_cost(sample)
        tight_budget = per_result * 3

        budget = TokenBudget(max_context_tokens=tight_budget, max_results=50)
        agent = TraversalAgent(token_budget=budget)
        state = agent.create_state("start")

        total_submitted = 0
        for hop in range(1, 4):
            results = [_make_result(f"n-{hop}-{i}", 0.1) for i in range(4)]
            total_submitted += len(results)
            step = TraversalStep(
                node_id=f"s-{hop}",
                hop_number=hop,
                results=results,
                new_frontier=[f"s-{hop + 1}"],
            )
            agent.record_step(state, step)

        assert len(state.accumulated_context) == total_submitted


class TestBackwardCompatibility:
    def test_output_is_list_of_dicts_with_expected_keys(self) -> None:
        budget = TokenBudget(max_context_tokens=10_000, max_results=50)
        agent = TraversalAgent(token_budget=budget)
        state = agent.create_state("start")

        results: List[Dict[str, Any]] = [
            _make_result("svc-a", 0.5),
            _make_result("svc-b", 0.8),
        ]
        step = TraversalStep(
            node_id="start",
            hop_number=1,
            results=results,
            new_frontier=[],
        )
        agent.record_step(state, step)

        context = agent.get_context(state)
        assert isinstance(context, list)
        for item in context:
            assert isinstance(item, dict)
            assert "target_id" in item
