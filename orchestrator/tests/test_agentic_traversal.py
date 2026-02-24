from __future__ import annotations

import pytest

from orchestrator.app.agentic_traversal import (
    MAX_HOPS,
    MAX_VISITED,
    TraversalAgent,
    TraversalState,
    TraversalStep,
    build_one_hop_cypher,
)
from orchestrator.app.context_manager import TokenBudget
from orchestrator.app.query_templates import ALLOWED_RELATIONSHIP_TYPES


class TestBuildOneHopCypher:
    def test_allowed_relationship_returns_valid_cypher(self) -> None:
        for rel in ALLOWED_RELATIONSHIP_TYPES:
            cypher = build_one_hop_cypher(rel)
            assert f":{rel}]" in cypher
            assert "$tenant_id" in cypher
            assert "tombstoned_at IS NULL" in cypher

    def test_disallowed_relationship_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Disallowed"):
            build_one_hop_cypher("DELETE_ALL")


class TestTraversalState:
    def test_should_continue_true_when_frontier_present(self) -> None:
        state = TraversalState(
            frontier=["node-1"],
            remaining_hops=3,
        )
        assert state.should_continue is True

    def test_should_continue_false_when_no_hops(self) -> None:
        state = TraversalState(
            frontier=["node-1"],
            remaining_hops=0,
        )
        assert state.should_continue is False

    def test_should_continue_false_when_frontier_empty(self) -> None:
        state = TraversalState(
            frontier=[],
            remaining_hops=3,
        )
        assert state.should_continue is False

    def test_should_continue_false_when_too_many_visited(self) -> None:
        state = TraversalState(
            visited_nodes=set(f"n-{i}" for i in range(MAX_VISITED)),
            frontier=["node-next"],
            remaining_hops=3,
        )
        assert state.should_continue is False

    def test_should_continue_false_when_token_budget_exhausted(self) -> None:
        budget = TokenBudget(max_context_tokens=10)
        state = TraversalState(
            frontier=["node-1"],
            remaining_hops=3,
            token_budget=budget,
            current_tokens=10,
        )
        assert state.should_continue is False


class TestTraversalAgent:
    def test_create_state_initializes_correctly(self) -> None:
        agent = TraversalAgent(max_hops=3, max_visited=10)
        state = agent.create_state("start-node")
        assert state.frontier == ["start-node"]
        assert state.remaining_hops == 3
        assert len(state.visited_nodes) == 0

    def test_max_hops_capped_at_global_limit(self) -> None:
        agent = TraversalAgent(max_hops=100)
        state = agent.create_state("n")
        assert state.remaining_hops == MAX_HOPS

    def test_select_next_node_returns_unvisited(self) -> None:
        agent = TraversalAgent()
        state = agent.create_state("a")
        state.frontier.extend(["b", "c"])
        node = agent.select_next_node(state)
        assert node == "a"

    def test_select_next_node_skips_visited(self) -> None:
        agent = TraversalAgent()
        state = agent.create_state("a")
        state.visited_nodes.add("a")
        state.frontier.append("b")
        node = agent.select_next_node(state)
        assert node == "b"

    def test_select_next_node_returns_none_when_empty(self) -> None:
        agent = TraversalAgent()
        state = TraversalState(frontier=[], remaining_hops=5)
        assert agent.select_next_node(state) is None

    def test_record_step_updates_state(self) -> None:
        agent = TraversalAgent()
        state = agent.create_state("start")
        step = TraversalStep(
            node_id="start",
            hop_number=1,
            results=[{"name": "svc-a"}],
            new_frontier=["svc-a"],
        )
        agent.record_step(state, step)
        assert "start" in state.visited_nodes
        assert state.remaining_hops == MAX_HOPS - 1
        assert len(state.accumulated_context) == 1
        assert "svc-a" in state.frontier

    def test_record_step_respects_token_budget(self) -> None:
        budget = TokenBudget(max_context_tokens=5)
        agent = TraversalAgent(token_budget=budget)
        state = agent.create_state("start")
        large_result = {"data": "x" * 100}
        step = TraversalStep(
            node_id="start",
            hop_number=1,
            results=[large_result],
            new_frontier=[],
        )
        agent.record_step(state, step)
        assert len(state.accumulated_context) == 0

    def test_get_context_returns_truncated_data(self) -> None:
        agent = TraversalAgent()
        state = agent.create_state("x")
        state.accumulated_context = [
            {"name": f"svc-{i}"} for i in range(10)
        ]
        ctx = agent.get_context(state)
        assert len(ctx) <= 10
