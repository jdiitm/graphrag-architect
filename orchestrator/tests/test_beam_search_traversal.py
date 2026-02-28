from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.agentic_traversal import (
    TraversalAgent,
    TraversalConfig,
    TraversalStrategy,
    TraversalStep,
    _batched_bfs,
    compute_node_score,
)
from orchestrator.app.context_manager import TokenBudget

_DEFAULT_ACL = {"is_admin": True, "acl_team": "", "acl_namespaces": []}


def _make_scored_results(source_id: str, count: int) -> list[dict]:
    return [
        {
            "source_id": source_id,
            "target_id": f"{source_id}-child-{i}",
            "target_name": f"svc-{i}",
            "rel_type": "CALLS",
            "target_label": "Service",
            "pagerank": float(i) / max(count, 1),
            "degree": i,
        }
        for i in range(count)
    ]


class TestBeamWidthEnforcement:
    def test_frontier_capped_at_beam_width_per_step(self) -> None:
        agent = TraversalAgent(beam_width=50)
        state = agent.create_state("start")

        results = _make_scored_results("start", 200)
        step = TraversalStep(
            node_id="start",
            hop_number=1,
            results=results,
            new_frontier=[r["target_id"] for r in results],
        )
        agent.record_step(state, step)

        assert len(state.frontier) <= 50

    def test_beam_width_10_strictly_caps(self) -> None:
        agent = TraversalAgent(beam_width=10)
        state = agent.create_state("start")

        results = _make_scored_results("start", 100)
        step = TraversalStep(
            node_id="start",
            hop_number=1,
            results=results,
            new_frontier=[r["target_id"] for r in results],
        )
        agent.record_step(state, step)

        assert len(state.frontier) == 10

    def test_frontier_preserved_when_below_beam_width(self) -> None:
        agent = TraversalAgent(beam_width=50)
        state = agent.create_state("start")

        results = _make_scored_results("start", 5)
        step = TraversalStep(
            node_id="start",
            hop_number=1,
            results=results,
            new_frontier=[r["target_id"] for r in results],
        )
        agent.record_step(state, step)

        assert len(state.frontier) == 5


class TestPriorityOrdering:
    def test_high_pagerank_explored_first(self) -> None:
        agent = TraversalAgent(beam_width=10)
        state = agent.create_state("start")

        results = [
            {"target_id": "low", "pagerank": 0.1, "degree": 5},
            {"target_id": "high", "pagerank": 0.9, "degree": 50},
            {"target_id": "mid", "pagerank": 0.5, "degree": 20},
        ]
        step = TraversalStep(
            node_id="start",
            hop_number=1,
            results=results,
            new_frontier=["low", "high", "mid"],
        )
        agent.record_step(state, step)

        first = agent.select_next_node(state)
        assert first == "high"

    def test_frontier_ordered_by_composite_score(self) -> None:
        agent = TraversalAgent(beam_width=5)
        state = agent.create_state("start")

        results = [
            {"target_id": "a", "pagerank": 0.1, "degree": 10},
            {"target_id": "b", "pagerank": 0.9, "degree": 100},
            {"target_id": "c", "pagerank": 0.5, "degree": 50},
            {"target_id": "d", "pagerank": 0.7, "degree": 30},
            {"target_id": "e", "pagerank": 0.3, "degree": 200},
        ]
        step = TraversalStep(
            node_id="start",
            hop_number=1,
            results=results,
            new_frontier=["a", "b", "c", "d", "e"],
        )
        agent.record_step(state, step)

        scores = {
            "a": 0.1 + 10 / 1000.0,
            "b": 0.9 + 100 / 1000.0,
            "c": 0.5 + 50 / 1000.0,
            "d": 0.7 + 30 / 1000.0,
            "e": 0.3 + 200 / 1000.0,
        }
        expected_order = sorted(scores, key=scores.get, reverse=True)

        actual_order = []
        while True:
            node = agent.select_next_node(state)
            if node is None:
                break
            actual_order.append(node)

        assert actual_order == expected_order

    def test_compute_node_score_formula(self) -> None:
        result = {"pagerank": 0.8, "degree": 400}
        score = compute_node_score(result)
        assert score == pytest.approx(0.8 + 400 / 1000.0)

    def test_compute_node_score_missing_fields(self) -> None:
        assert compute_node_score({}) == 0.0
        assert compute_node_score({"pagerank": 0.5}) == pytest.approx(0.5)
        assert compute_node_score({"degree": 100}) == pytest.approx(0.1)


class TestFanOutCeiling:
    def test_frontier_bounded_across_hops(self) -> None:
        agent = TraversalAgent(beam_width=10, max_hops=3)
        state = agent.create_state("start")

        for hop in range(3):
            current = agent.select_next_node(state)
            if current is None:
                break

            results = [
                {
                    "target_id": f"h{hop}-n{i}",
                    "pagerank": float(i) / 200,
                    "degree": i,
                }
                for i in range(200)
            ]
            step = TraversalStep(
                node_id=current,
                hop_number=hop + 1,
                results=results,
                new_frontier=[f"h{hop}-n{i}" for i in range(200)],
            )
            agent.record_step(state, step)

            assert len(state.frontier) <= 10

    def test_beam_width_prevents_exponential_blowup(self) -> None:
        agent = TraversalAgent(beam_width=10, max_hops=3)
        state = agent.create_state("start")

        all_discovered: set[str] = set()
        for hop in range(3):
            current = agent.select_next_node(state)
            if current is None:
                break

            results = [
                {
                    "target_id": f"h{hop}-n{i}",
                    "pagerank": float(i) / 200,
                    "degree": i,
                }
                for i in range(200)
            ]
            step = TraversalStep(
                node_id=current,
                hop_number=hop + 1,
                results=results,
                new_frontier=[f"h{hop}-n{i}" for i in range(200)],
            )
            agent.record_step(state, step)
            all_discovered.update(state.frontier)

        assert len(all_discovered) <= 30

    def test_highest_scored_retained_after_truncation(self) -> None:
        agent = TraversalAgent(beam_width=5, max_hops=2)
        state = agent.create_state("start")

        results = [
            {"target_id": f"n-{i}", "pagerank": float(i) / 100, "degree": i}
            for i in range(100)
        ]
        step = TraversalStep(
            node_id="start",
            hop_number=1,
            results=results,
            new_frontier=[f"n-{i}" for i in range(100)],
        )
        agent.record_step(state, step)

        for nid in state.frontier:
            idx = int(nid.split("-")[1])
            assert idx >= 95


class TestBackwardCompatibility:
    def test_config_beam_width_default(self) -> None:
        config = TraversalConfig()
        assert config.beam_width == 50

    def test_config_custom_beam_width(self) -> None:
        config = TraversalConfig(beam_width=25)
        assert config.beam_width == 25

    def test_config_from_env_beam_width(self) -> None:
        env = {"TRAVERSAL_BEAM_WIDTH": "30"}
        with patch.dict("os.environ", env):
            config = TraversalConfig.from_env()
        assert config.beam_width == 30

    def test_default_agent_small_frontier_preserved(self) -> None:
        agent = TraversalAgent(beam_width=50)
        state = agent.create_state("start")

        results = [{"target_id": "a", "name": "svc-a"}]
        step = TraversalStep(
            node_id="start",
            hop_number=1,
            results=results,
            new_frontier=["a"],
        )
        agent.record_step(state, step)

        assert "a" in state.frontier
        assert len(state.accumulated_context) == 1

        ctx = agent.get_context(state)
        assert isinstance(ctx, list)

    def test_results_format_unchanged(self) -> None:
        agent = TraversalAgent(beam_width=50)
        state = agent.create_state("start")

        results = [
            {"target_id": "svc-a", "target_name": "auth", "target_label": "Service"},
        ]
        step = TraversalStep(
            node_id="start",
            hop_number=1,
            results=results,
            new_frontier=["svc-a"],
        )
        agent.record_step(state, step)

        ctx = agent.get_context(state)
        assert len(ctx) == 1
        assert ctx[0]["target_id"] == "svc-a"
        assert ctx[0]["target_name"] == "auth"


def _make_beam_mock_driver(hop_data_sequence: list[list[dict]]) -> AsyncMock:
    call_index = {"i": 0}

    async def _mock_run(query, **params):
        idx = call_index["i"]
        call_index["i"] += 1
        result = AsyncMock()
        data = hop_data_sequence[idx] if idx < len(hop_data_sequence) else []
        result.data = AsyncMock(return_value=data)
        return result

    mock_tx = AsyncMock()
    mock_tx.run = _mock_run

    mock_session = AsyncMock()

    async def _execute_read(func, **kwargs):
        return await func(mock_tx)

    mock_session.execute_read = _execute_read
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    mock_driver = AsyncMock()
    mock_driver.session = MagicMock(return_value=mock_session)

    return mock_driver


@pytest.mark.asyncio
class TestBatchedBFSBeamIntegration:
    async def test_batched_bfs_accepts_beam_width(self) -> None:
        hop1 = _make_scored_results("start", 200)
        driver = _make_beam_mock_driver([hop1, []])

        results = await _batched_bfs(
            driver=driver,
            start_node_id="start",
            tenant_id="t1",
            acl_params=_DEFAULT_ACL,
            max_hops=2,
            timeout=10.0,
            token_budget=TokenBudget(),
            beam_width=10,
        )

        assert isinstance(results, list)

    async def test_beam_width_flows_through_run_traversal(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.BATCHED_BFS,
            beam_width=15,
        )

        with patch(
            "orchestrator.app.agentic_traversal._batched_bfs",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_bfs:
            from orchestrator.app.agentic_traversal import run_traversal

            mock_driver = MagicMock()
            await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                config=config,
            )
            assert mock_bfs.call_args.kwargs["beam_width"] == 15
