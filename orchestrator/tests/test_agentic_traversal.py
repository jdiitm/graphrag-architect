from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from neo4j.exceptions import ClientError

from orchestrator.app.agentic_traversal import (
    MAX_HOPS,
    MAX_VISITED,
    TraversalAgent,
    TraversalConfig,
    TraversalState,
    TraversalStep,
    TraversalStrategy,
    build_one_hop_cypher,
    run_traversal,
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

    def test_should_continue_false_when_collection_ceiling_reached(self) -> None:
        budget = TokenBudget(max_context_tokens=10)
        state = TraversalState(
            frontier=["node-1"],
            remaining_hops=3,
            token_budget=budget,
            current_tokens=20,
        )
        assert state.should_continue is False

    def test_should_continue_respects_custom_max_visited(self) -> None:
        state = TraversalState(
            visited_nodes=set(f"n-{i}" for i in range(5)),
            frontier=["node-next"],
            remaining_hops=3,
            max_visited=5,
        )
        assert state.should_continue is False

    def test_should_continue_true_below_custom_max_visited(self) -> None:
        state = TraversalState(
            visited_nodes=set(f"n-{i}" for i in range(4)),
            frontier=["node-next"],
            remaining_hops=3,
            max_visited=5,
        )
        assert state.should_continue is True


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

    def test_record_step_collects_all_then_get_context_truncates(self) -> None:
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
        assert len(state.accumulated_context) == 1
        context = agent.get_context(state)
        assert len(context) == 0

    def test_get_context_returns_truncated_data(self) -> None:
        agent = TraversalAgent()
        state = agent.create_state("x")
        state.accumulated_context = [
            {"name": f"svc-{i}"} for i in range(10)
        ]
        ctx = agent.get_context(state)
        assert len(ctx) <= 10


class TestTraversalStrategyEnum:
    def test_traversal_strategy_enum_values(self) -> None:
        assert TraversalStrategy.BOUNDED_CYPHER.value == "bounded_cypher"
        assert TraversalStrategy.BATCHED_BFS.value == "batched_bfs"
        assert TraversalStrategy.ADAPTIVE.value == "adaptive"


class TestTraversalConfig:
    def test_traversal_config_defaults(self) -> None:
        config = TraversalConfig()
        assert config.strategy == TraversalStrategy.ADAPTIVE
        assert config.degree_threshold == 200
        assert config.max_hops == MAX_HOPS
        assert config.max_visited == MAX_VISITED
        assert config.timeout == 30.0

    def test_traversal_config_from_env(self) -> None:
        env = {
            "TRAVERSAL_STRATEGY": "batched_bfs",
            "TRAVERSAL_DEGREE_THRESHOLD": "500",
            "TRAVERSAL_MAX_HOPS": "3",
            "TRAVERSAL_MAX_VISITED": "25",
            "TRAVERSAL_TIMEOUT": "10.0",
        }
        with patch.dict("os.environ", env):
            config = TraversalConfig.from_env()
        assert config.strategy == TraversalStrategy.BATCHED_BFS
        assert config.degree_threshold == 500
        assert config.max_hops == 3
        assert config.max_visited == 25
        assert config.timeout == 10.0


class TestDegreeHintFallback:
    @pytest.mark.asyncio
    async def test_adaptive_with_none_hint_defaults_to_bounded(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.ADAPTIVE,
            degree_threshold=200,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()
        expected = [{"target_id": "svc-default"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal._try_apoc_expansion",
                new_callable=AsyncMock,
                side_effect=ClientError("not available"),
            ),
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=expected,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
                degree_hint=None,
            )
            mock_bfs.assert_called_once()
            assert result == expected

    @pytest.mark.asyncio
    async def test_adaptive_with_explicit_degree_routes_correctly(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.ADAPTIVE,
            degree_threshold=200,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()
        expected = [{"target_id": "svc-dense"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal._try_apoc_expansion",
                new_callable=AsyncMock,
                side_effect=ClientError("not available"),
            ),
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=expected,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
                degree_hint=500,
            )
            mock_bfs.assert_called_once()
            assert result == expected


class TestRunTraversalStrategySelection:
    @pytest.mark.asyncio
    async def test_adaptive_selects_bounded_for_low_degree(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.ADAPTIVE,
            degree_threshold=200,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()
        expected = [{"target_id": "svc-b", "target_name": "svc-b"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal._try_apoc_expansion",
                new_callable=AsyncMock,
                side_effect=ClientError("not available"),
            ),
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=expected,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
                degree_hint=50,
            )
            mock_bfs.assert_called_once()
            assert result == expected

    @pytest.mark.asyncio
    async def test_adaptive_selects_batched_for_high_degree(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.ADAPTIVE,
            degree_threshold=200,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()
        expected = [{"target_id": "svc-c"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal._try_apoc_expansion",
                new_callable=AsyncMock,
                side_effect=ClientError("not available"),
            ),
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=expected,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
                degree_hint=500,
            )
            mock_bfs.assert_called_once()
            assert result == expected

    @pytest.mark.asyncio
    async def test_bounded_strategy_always_uses_bounded(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.BOUNDED_CYPHER,
            degree_threshold=10,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()
        expected = [{"target_id": "svc-d"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal.bounded_path_expansion",
                new_callable=AsyncMock,
                return_value=expected,
            ) as mock_bounded,
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
            )
            mock_bounded.assert_called_once()
            mock_bfs.assert_not_called()
            assert result == expected

    @pytest.mark.asyncio
    async def test_batched_strategy_always_uses_batched(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.BATCHED_BFS,
            degree_threshold=10,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()
        expected = [{"target_id": "svc-e"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal.bounded_path_expansion",
                new_callable=AsyncMock,
            ) as mock_bounded,
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=expected,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
            )
            mock_bounded.assert_not_called()
            mock_bfs.assert_called_once()
            assert result == expected

    @pytest.mark.asyncio
    async def test_config_max_visited_flows_to_batched_bfs(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.BATCHED_BFS,
            max_visited=10,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()

        with patch(
            "orchestrator.app.agentic_traversal._batched_bfs",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_bfs:
            await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
            )
            assert mock_bfs.call_args.kwargs["max_visited"] == 10

    @pytest.mark.asyncio
    async def test_config_max_visited_flows_to_bounded_expansion(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.BOUNDED_CYPHER,
            max_visited=10,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()

        with patch(
            "orchestrator.app.agentic_traversal.bounded_path_expansion",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_bounded:
            await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
            )
            assert mock_bounded.call_args.kwargs["max_nodes"] == 10

    @pytest.mark.asyncio
    async def test_unknown_strategy_raises_value_error(self) -> None:
        config = TraversalConfig()
        object.__setattr__(config, "strategy", "not_a_real_strategy")
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()

        with pytest.raises(ValueError, match="Unknown traversal strategy"):
            await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
            )

    @pytest.mark.asyncio
    async def test_config_clamps_max_hops_when_config_is_lower(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.BATCHED_BFS,
            max_hops=2,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()

        with patch(
            "orchestrator.app.agentic_traversal._batched_bfs",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_bfs:
            await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                max_hops=5,
                config=config,
            )
            assert mock_bfs.call_args.kwargs["max_hops"] == 2

    @pytest.mark.asyncio
    async def test_config_clamps_max_hops_when_arg_is_lower(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.BATCHED_BFS,
            max_hops=5,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()

        with patch(
            "orchestrator.app.agentic_traversal._batched_bfs",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_bfs:
            await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                max_hops=3,
                config=config,
            )
            assert mock_bfs.call_args.kwargs["max_hops"] == 3

    @pytest.mark.asyncio
    async def test_config_clamps_timeout_when_config_is_lower(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.BATCHED_BFS,
            timeout=10.0,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()

        with patch(
            "orchestrator.app.agentic_traversal._batched_bfs",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_bfs:
            await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                timeout=30.0,
                config=config,
            )
            assert mock_bfs.call_args.kwargs["timeout"] == 10.0

    @pytest.mark.asyncio
    async def test_adaptive_fallback_on_bounded_error(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.ADAPTIVE,
            degree_threshold=200,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()
        fallback_result = [{"target_id": "svc-f"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal._try_apoc_expansion",
                new_callable=AsyncMock,
                side_effect=ClientError("not available"),
            ),
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=fallback_result,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
                degree_hint=50,
            )
            mock_bfs.assert_called_once()
            assert result == fallback_result
