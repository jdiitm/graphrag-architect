from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from neo4j.exceptions import ClientError

from orchestrator.app.agentic_traversal import (
    TraversalConfig,
    TraversalStrategy,
    run_traversal,
)


_DEFAULT_ACL: dict = {"is_admin": True, "acl_team": "", "acl_namespaces": []}


def _mock_tx_two_queries(
    node_records: list,
    edge_records: list,
) -> AsyncMock:
    tx = AsyncMock()
    node_result = AsyncMock()
    node_result.data = AsyncMock(return_value=node_records)
    edge_result = AsyncMock()
    edge_result.data = AsyncMock(return_value=edge_records)
    tx.run = AsyncMock(side_effect=[node_result, edge_result])
    return tx


def _mock_async_session_driver() -> tuple[MagicMock, AsyncMock]:
    driver = MagicMock()
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    driver.session.return_value = session
    return driver, session


class TestApocQueryTenantIsolation:
    @pytest.mark.asyncio
    async def test_node_query_filters_by_tenant_id(self) -> None:
        from orchestrator.app.agentic_traversal import apoc_path_expansion

        tx = _mock_tx_two_queries([], [])
        await apoc_path_expansion(
            tx,
            source_id="auth-service",
            tenant_id="tenant-42",
            acl_params=_DEFAULT_ACL,
        )

        assert tx.run.call_count == 2
        node_query = tx.run.call_args_list[0][0][0]
        assert "tenant_id" in node_query
        assert "$source_id" in node_query
        node_kwargs = tx.run.call_args_list[0][1]
        assert node_kwargs["tenant_id"] == "tenant-42"
        assert node_kwargs["source_id"] == "auth-service"

    @pytest.mark.asyncio
    async def test_node_query_enforces_acl(self) -> None:
        from orchestrator.app.agentic_traversal import apoc_path_expansion

        acl = {"is_admin": False, "acl_team": "platform", "acl_namespaces": ["prod"]}
        tx = _mock_tx_two_queries([], [])
        await apoc_path_expansion(
            tx,
            source_id="auth-service",
            tenant_id="t1",
            acl_params=acl,
        )

        node_query = tx.run.call_args_list[0][0][0]
        assert "$is_admin" in node_query
        assert "$acl_team" in node_query
        assert "$acl_namespaces" in node_query

    @pytest.mark.asyncio
    async def test_edge_query_filters_tombstoned(self) -> None:
        from orchestrator.app.agentic_traversal import apoc_path_expansion

        tx = _mock_tx_two_queries([], [])
        await apoc_path_expansion(
            tx,
            source_id="svc-a",
            tenant_id="t1",
            acl_params=_DEFAULT_ACL,
        )

        edge_query = tx.run.call_args_list[1][0][0]
        assert "tombstoned_at IS NULL" in edge_query


class TestApocPropertyBasedIds:
    @pytest.mark.asyncio
    async def test_queries_use_property_id_not_element_id(self) -> None:
        from orchestrator.app.agentic_traversal import apoc_path_expansion

        tx = _mock_tx_two_queries(
            [{"node_id": "auth-service", "labels": ["Service"]}],
            [],
        )
        result = await apoc_path_expansion(
            tx,
            source_id="auth-service",
            tenant_id="t1",
            acl_params=_DEFAULT_ACL,
        )

        for call in tx.run.call_args_list:
            query = call[0][0]
            assert "elementId" not in query
            assert "{id: $source_id" in query
        assert "auth-service" in result["nodes"]


class TestApocEdgeDeduplication:
    @pytest.mark.asyncio
    async def test_duplicate_edges_are_collapsed(self) -> None:
        from orchestrator.app.agentic_traversal import apoc_path_expansion

        nodes = [
            {"node_id": "a", "labels": ["Service"]},
            {"node_id": "b", "labels": ["Service"]},
        ]
        edges = [
            {"src": "a", "tgt": "b", "rel_type": "CALLS"},
            {"src": "a", "tgt": "b", "rel_type": "CALLS"},
        ]
        tx = _mock_tx_two_queries(nodes, edges)
        result = await apoc_path_expansion(
            tx, source_id="a", tenant_id="t1", acl_params=_DEFAULT_ACL,
        )

        assert len(result["edges"]) == 1
        assert result["edges"][0] == ("a", "b", "CALLS")


class TestApocEdgeCrossFilter:
    @pytest.mark.asyncio
    async def test_edges_excluded_when_endpoint_not_in_valid_nodes(self) -> None:
        from orchestrator.app.agentic_traversal import apoc_path_expansion

        nodes = [{"node_id": "a", "labels": ["Service"]}]
        edges = [
            {"src": "a", "tgt": "b", "rel_type": "CALLS"},
            {"src": "a", "tgt": "a", "rel_type": "SELF_REF"},
        ]
        tx = _mock_tx_two_queries(nodes, edges)
        result = await apoc_path_expansion(
            tx, source_id="a", tenant_id="t1", acl_params=_DEFAULT_ACL,
        )

        assert ("a", "b", "CALLS") not in result["edges"]
        assert ("a", "a", "SELF_REF") in result["edges"]


class TestApocQueryUsesExpandConfig:
    @pytest.mark.asyncio
    async def test_both_queries_contain_apoc_expand_config(self) -> None:
        from orchestrator.app.agentic_traversal import apoc_path_expansion

        tx = _mock_tx_two_queries([], [])
        await apoc_path_expansion(
            tx,
            source_id="svc-a",
            tenant_id="t1",
            acl_params=_DEFAULT_ACL,
            max_hops=3,
            max_visited=500,
        )

        assert tx.run.call_count == 2
        for call in tx.run.call_args_list:
            query = call[0][0]
            assert "apoc.path.expandConfig" in query
            kwargs = call[1]
            assert kwargs["max_hops"] == 3
            assert kwargs["max_visited"] == 500


class TestApocResultStructure:
    @pytest.mark.asyncio
    async def test_returns_nodes_dict_and_edges_list(self) -> None:
        from orchestrator.app.agentic_traversal import apoc_path_expansion

        nodes = [
            {"node_id": "svc-a", "labels": ["Service"]},
            {"node_id": "svc-b", "labels": ["Service"]},
        ]
        edges = [{"src": "svc-a", "tgt": "svc-b", "rel_type": "DEPENDS_ON"}]
        tx = _mock_tx_two_queries(nodes, edges)
        result = await apoc_path_expansion(
            tx, source_id="svc-a", tenant_id="t1", acl_params=_DEFAULT_ACL,
        )

        assert "nodes" in result
        assert "edges" in result
        assert "svc-b" in result["nodes"]
        assert result["nodes"]["svc-b"] == ["Service"]
        assert len(result["edges"]) == 1
        assert result["edges"][0] == ("svc-a", "svc-b", "DEPENDS_ON")

    @pytest.mark.asyncio
    async def test_multiple_property_node_ids_and_rel_types(self) -> None:
        from orchestrator.app.agentic_traversal import apoc_path_expansion

        nodes = [
            {"node_id": "pod-a", "labels": ["Pod"]},
            {"node_id": "node-b", "labels": ["Node"]},
            {"node_id": "cluster-c", "labels": ["Cluster"]},
        ]
        edges = [
            {"src": "pod-a", "tgt": "node-b", "rel_type": "RUNS_ON"},
            {"src": "node-b", "tgt": "cluster-c", "rel_type": "HOSTS"},
        ]
        tx = _mock_tx_two_queries(nodes, edges)
        result = await apoc_path_expansion(
            tx, source_id="pod-a", tenant_id="t1", acl_params=_DEFAULT_ACL,
        )

        assert "pod-a" in result["nodes"]
        assert "node-b" in result["nodes"]
        assert "cluster-c" in result["nodes"]
        assert ("pod-a", "node-b", "RUNS_ON") in result["edges"]
        assert ("node-b", "cluster-c", "HOSTS") in result["edges"]
        assert len(result["edges"]) == 2


class TestTraversalStrategyApocEnum:
    def test_apoc_enum_value_exists(self) -> None:
        assert TraversalStrategy.APOC.value == "apoc"
        assert TraversalStrategy("apoc") == TraversalStrategy.APOC


class TestApocStrategyPassesTenantParams:
    @pytest.mark.asyncio
    async def test_apoc_routing_forwards_tenant_and_acl(self) -> None:
        config = TraversalConfig(strategy=TraversalStrategy.APOC)
        driver, _ = _mock_async_session_driver()
        formatted = [{"source_id": "svc-a", "target_id": "svc-b", "rel_type": "CALLS"}]

        with patch(
            "orchestrator.app.agentic_traversal._try_apoc_expansion",
            new_callable=AsyncMock,
            return_value=formatted,
        ) as mock_try:
            result = await run_traversal(
                driver=driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                config=config,
            )
            mock_try.assert_called_once()
            call_kwargs = mock_try.call_args.kwargs
            assert call_kwargs["tenant_id"] == "t1"
            assert call_kwargs["acl_params"] == _DEFAULT_ACL
            assert isinstance(result, list)


class TestAdaptiveWithoutDegreeHintFallsBack:
    @pytest.mark.asyncio
    async def test_no_degree_hint_tries_apoc_then_bfs(self) -> None:
        config = TraversalConfig(strategy=TraversalStrategy.ADAPTIVE)
        driver, _ = _mock_async_session_driver()
        bfs_result = [{"target_id": "svc-fallback"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal._try_apoc_expansion",
                new_callable=AsyncMock,
                side_effect=ClientError("Procedure not found"),
            ) as mock_apoc,
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=bfs_result,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                config=config,
            )
            mock_apoc.assert_called_once()
            mock_bfs.assert_called_once()
            assert result == bfs_result


class TestAdaptiveDegreeHintRouting:
    @pytest.mark.asyncio
    async def test_high_degree_hint_skips_apoc_routes_to_bfs(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.ADAPTIVE,
            degree_threshold=200,
        )
        driver, _ = _mock_async_session_driver()
        bfs_result = [{"target_id": "svc-dense"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal._try_apoc_expansion",
                new_callable=AsyncMock,
            ) as mock_apoc,
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=bfs_result,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                config=config,
                degree_hint=500,
            )
            mock_apoc.assert_not_called()
            mock_bfs.assert_called_once()
            assert result == bfs_result

    @pytest.mark.asyncio
    async def test_low_degree_hint_skips_apoc_routes_to_bounded_cypher(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.ADAPTIVE,
            degree_threshold=200,
        )
        driver, _ = _mock_async_session_driver()
        bounded_result = [{"target_id": "svc-sparse"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal._try_apoc_expansion",
                new_callable=AsyncMock,
            ) as mock_apoc,
            patch(
                "orchestrator.app.agentic_traversal._run_bounded_with_fallback",
                new_callable=AsyncMock,
                return_value=bounded_result,
            ) as mock_bounded,
        ):
            result = await run_traversal(
                driver=driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                config=config,
                degree_hint=50,
            )
            mock_apoc.assert_not_called()
            mock_bounded.assert_called_once()
            assert result == bounded_result


class TestApocUsesManagedTransaction:
    @pytest.mark.asyncio
    async def test_try_apoc_calls_execute_read_not_session_run(self) -> None:
        from orchestrator.app.agentic_traversal import _try_apoc_expansion
        from orchestrator.app.context_manager import TokenBudget

        driver, session = _mock_async_session_driver()
        apoc_raw = {"nodes": {}, "edges": []}
        session.execute_read = AsyncMock(return_value=apoc_raw)

        await _try_apoc_expansion(
            driver=driver,
            start_node_id="svc-a",
            tenant_id="t1",
            acl_params=_DEFAULT_ACL,
            max_hops=3,
            max_visited=50,
            token_budget=TokenBudget(),
        )

        session.execute_read.assert_called_once()
        session.run.assert_not_called()


class TestApocEmptyResult:
    @pytest.mark.asyncio
    async def test_empty_expansion_returns_empty_collections(self) -> None:
        from orchestrator.app.agentic_traversal import apoc_path_expansion

        tx = _mock_tx_two_queries([], [])
        result = await apoc_path_expansion(
            tx, source_id="nonexistent", tenant_id="t1", acl_params=_DEFAULT_ACL,
        )

        assert result["nodes"] == {}
        assert result["edges"] == []


class TestExistingStrategiesUnaffected:
    @pytest.mark.asyncio
    async def test_bounded_cypher_does_not_invoke_apoc(self) -> None:
        config = TraversalConfig(strategy=TraversalStrategy.BOUNDED_CYPHER)
        mock_driver = MagicMock()

        with (
            patch(
                "orchestrator.app.agentic_traversal.bounded_path_expansion",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "orchestrator.app.agentic_traversal.apoc_path_expansion",
                new_callable=AsyncMock,
            ) as mock_apoc,
        ):
            await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                config=config,
            )
            mock_apoc.assert_not_called()

    @pytest.mark.asyncio
    async def test_batched_bfs_does_not_invoke_apoc(self) -> None:
        config = TraversalConfig(strategy=TraversalStrategy.BATCHED_BFS)
        mock_driver = MagicMock()

        with (
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "orchestrator.app.agentic_traversal.apoc_path_expansion",
                new_callable=AsyncMock,
            ) as mock_apoc,
        ):
            await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                config=config,
            )
            mock_apoc.assert_not_called()


class TestTraversalStrategyEnvConfig:
    def test_apoc_env_var_selects_apoc_strategy(self) -> None:
        with patch.dict("os.environ", {"TRAVERSAL_STRATEGY": "apoc"}):
            config = TraversalConfig.from_env()
        assert config.strategy == TraversalStrategy.APOC
