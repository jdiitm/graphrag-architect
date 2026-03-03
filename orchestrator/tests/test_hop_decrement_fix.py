from __future__ import annotations

import pytest

from orchestrator.app.agentic_traversal import (
    MAX_HOPS,
    TraversalAgent,
    TraversalState,
    TraversalStep,
    _record_batched_results,
)
from orchestrator.app.context_manager import TokenBudget


class TestHopDecrementPerLevel:
    """Verify that hop budget decrements once per BFS level, not per node."""

    def test_batch_of_n_nodes_consumes_one_hop(self) -> None:
        agent = TraversalAgent(max_hops=5)
        state = agent.create_state("root")
        state.remaining_hops = 5

        frontier_batch = [f"node-{i}" for i in range(10)]
        results = [
            {"source_id": nid, "target_id": f"child-{nid}", "name": nid}
            for nid in frontier_batch
        ]

        _record_batched_results(agent, state, frontier_batch, results, hop_number=1)

        assert state.remaining_hops == 5, (
            "record_step must NOT decrement remaining_hops; "
            "the caller (_batched_bfs) owns hop accounting"
        )

    def test_five_bfs_levels_consume_five_hops(self) -> None:
        agent = TraversalAgent(max_hops=5, max_visited=200)
        state = agent.create_state("root")
        state.remaining_hops = 5

        for level in range(5):
            frontier_batch = [f"L{level}-n{i}" for i in range(3)]
            results = [
                {
                    "source_id": nid,
                    "target_id": f"L{level + 1}-n{i}",
                    "name": nid,
                }
                for i, nid in enumerate(frontier_batch)
            ]
            _record_batched_results(
                agent, state, frontier_batch, results, hop_number=level + 1,
            )
            state.remaining_hops -= 1

        assert state.remaining_hops == 0

    def test_record_step_does_not_touch_remaining_hops(self) -> None:
        agent = TraversalAgent(max_hops=5)
        state = agent.create_state("start")
        initial_hops = state.remaining_hops

        step = TraversalStep(
            node_id="start",
            hop_number=1,
            results=[{"name": "svc-a"}],
            new_frontier=["svc-a"],
        )
        agent.record_step(state, step)

        assert state.remaining_hops == initial_hops, (
            "record_step must not modify remaining_hops"
        )

    def test_single_node_frontier_still_costs_one_hop(self) -> None:
        agent = TraversalAgent(max_hops=3)
        state = agent.create_state("root")
        state.remaining_hops = 3

        _record_batched_results(
            agent,
            state,
            ["only-node"],
            [{"source_id": "only-node", "target_id": "child", "name": "x"}],
            hop_number=1,
        )

        assert state.remaining_hops == 3, (
            "record_step must not decrement; caller decrements per level"
        )

    def test_large_frontier_preserves_hop_budget(self) -> None:
        agent = TraversalAgent(max_hops=5, max_visited=500)
        state = agent.create_state("root")
        state.remaining_hops = 5

        frontier_batch = [f"node-{i}" for i in range(100)]
        results = [
            {"source_id": nid, "target_id": f"child-{nid}", "name": nid}
            for nid in frontier_batch
        ]
        _record_batched_results(agent, state, frontier_batch, results, hop_number=1)

        assert state.remaining_hops == 5, (
            f"100-node batch must not consume 100 hops; "
            f"got remaining_hops={state.remaining_hops}"
        )
