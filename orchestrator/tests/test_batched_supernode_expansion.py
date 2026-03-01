from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.agentic_traversal import (
    MAX_NODE_DEGREE,
    TraversalConfig,
    TraversalStrategy,
    _batched_supernode_expansion,
    execute_batched_hop,
    run_traversal,
)

_DEFAULT_ACL = {
    "is_admin": True,
    "acl_team": "",
    "acl_namespaces": [],
}


def _make_supernode_mock_driver(
    results: list[dict],
) -> tuple[AsyncMock, AsyncMock]:
    mock_tx = AsyncMock()
    mock_result = AsyncMock()
    mock_result.data = AsyncMock(return_value=results)
    mock_tx.run = AsyncMock(return_value=mock_result)

    mock_session = AsyncMock()

    async def _execute_read(func, **kwargs):
        return await func(mock_tx)

    mock_session.execute_read = _execute_read
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    mock_driver = AsyncMock()
    mock_driver.session = MagicMock(return_value=mock_session)

    return mock_driver, mock_tx


@pytest.mark.asyncio
class TestBatchedSupernodeExpansionReturnsNeighbors:

    async def test_returns_neighbors_for_multiple_supernodes(self) -> None:
        expected = [
            {"source_id": "hub-1", "target_id": "a", "target_name": "svc-a",
             "rel_type": "CALLS", "target_label": "Service",
             "pagerank": 0, "degree": 0},
            {"source_id": "hub-2", "target_id": "b", "target_name": "svc-b",
             "rel_type": "WRITES_TO", "target_label": "Topic",
             "pagerank": 0, "degree": 0},
            {"source_id": "hub-3", "target_id": "c", "target_name": "svc-c",
             "rel_type": "CALLS", "target_label": "Service",
             "pagerank": 0, "degree": 0},
        ]
        driver, _ = _make_supernode_mock_driver(expected)

        results = await _batched_supernode_expansion(
            driver=driver,
            source_ids=["hub-1", "hub-2", "hub-3"],
            tenant_id="t1",
            acl_params=_DEFAULT_ACL,
        )

        assert len(results) == 3
        source_ids = {r["source_id"] for r in results}
        assert source_ids == {"hub-1", "hub-2", "hub-3"}


@pytest.mark.asyncio
class TestBatchedSupernodeSingleQuery:

    async def test_single_cypher_query_for_n_supernodes(self) -> None:
        expected = [
            {"source_id": "s1", "target_id": "t1_target",
             "target_name": "svc-1", "rel_type": "CALLS",
             "target_label": "Service", "pagerank": 0, "degree": 0},
            {"source_id": "s2", "target_id": "t2_target",
             "target_name": "svc-2", "rel_type": "CALLS",
             "target_label": "Service", "pagerank": 0, "degree": 0},
        ]
        driver, mock_tx = _make_supernode_mock_driver(expected)

        await _batched_supernode_expansion(
            driver=driver,
            source_ids=["s1", "s2", "s3", "s4", "s5"],
            tenant_id="t1",
            acl_params=_DEFAULT_ACL,
        )

        assert mock_tx.run.call_count == 1, (
            f"Expected exactly 1 Cypher query for 5 supernodes, "
            f"got {mock_tx.run.call_count}"
        )


@pytest.mark.asyncio
class TestBatchedSupernodeTenantScoping:

    async def test_tenant_id_in_query_params(self) -> None:
        driver, mock_tx = _make_supernode_mock_driver([])

        await _batched_supernode_expansion(
            driver=driver,
            source_ids=["hub-1"],
            tenant_id="tenant-42",
            acl_params=_DEFAULT_ACL,
        )

        call_kwargs = mock_tx.run.call_args[1]
        assert call_kwargs["tenant_id"] == "tenant-42"

        query_text = mock_tx.run.call_args[0][0]
        assert "$tenant_id" in query_text


@pytest.mark.asyncio
class TestBatchedSupernodeSamplingLimits:

    async def test_sample_size_in_query_params(self) -> None:
        driver, mock_tx = _make_supernode_mock_driver([])

        await _batched_supernode_expansion(
            driver=driver,
            source_ids=["hub-1"],
            tenant_id="t1",
            acl_params=_DEFAULT_ACL,
            sample_size=25,
        )

        call_kwargs = mock_tx.run.call_args[1]
        assert call_kwargs["sample_size"] == 25

        query_text = mock_tx.run.call_args[0][0]
        assert "$sample_size" in query_text


@pytest.mark.asyncio
class TestBatchedSupernodeEmptyList:

    async def test_empty_source_ids_returns_empty(self) -> None:
        driver = AsyncMock()

        results = await _batched_supernode_expansion(
            driver=driver,
            source_ids=[],
            tenant_id="t1",
            acl_params=_DEFAULT_ACL,
        )

        assert results == []


@pytest.mark.asyncio
class TestBatchedHopRoutesSupernodesCorrectly:

    async def test_supernodes_routed_to_batched_expansion(self) -> None:
        degree_map = {
            "normal": 10,
            "supernode-1": MAX_NODE_DEGREE + 100,
            "supernode-2": MAX_NODE_DEGREE + 200,
        }

        async def _mock_batch_check(driver, source_ids, tenant_id, timeout=30.0):
            return {sid: degree_map.get(sid, 0) for sid in source_ids}

        batch_results = [
            {"source_id": "normal", "target_id": "from-batch",
             "target_name": "b", "rel_type": "CALLS",
             "target_label": "Service", "pagerank": 0, "degree": 0},
        ]
        supernode_results = [
            {"source_id": "supernode-1", "target_id": "from-super-1",
             "target_name": "s1", "rel_type": "CALLS",
             "target_label": "Service", "pagerank": 0, "degree": 0},
            {"source_id": "supernode-2", "target_id": "from-super-2",
             "target_name": "s2", "rel_type": "CALLS",
             "target_label": "Service", "pagerank": 0, "degree": 0},
        ]

        with patch(
            "orchestrator.app.agentic_traversal.batch_check_degrees",
            side_effect=_mock_batch_check,
        ), patch(
            "orchestrator.app.agentic_traversal._batched_supernode_expansion",
            new_callable=AsyncMock,
            return_value=supernode_results,
        ) as mock_super_expand, patch(
            "orchestrator.app.agentic_traversal.execute_hop",
            new_callable=AsyncMock,
        ) as mock_individual_hop:
            mock_session = AsyncMock()

            async def _batch_read(func, **kwargs):
                mock_tx = AsyncMock()

                async def _run(query, **params):
                    result = AsyncMock()
                    result.data = AsyncMock(return_value=batch_results)
                    return result

                mock_tx.run = _run
                return await func(mock_tx)

            mock_session.execute_read = _batch_read
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_driver = AsyncMock()
            mock_driver.session = MagicMock(return_value=mock_session)

            results = await execute_batched_hop(
                driver=mock_driver,
                source_ids=["normal", "supernode-1", "supernode-2"],
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
            )

            mock_super_expand.assert_called_once()
            call_kwargs = mock_super_expand.call_args[1]
            assert set(call_kwargs["source_ids"]) == {
                "supernode-1", "supernode-2",
            }

            mock_individual_hop.assert_not_called()

            target_ids = {r["target_id"] for r in results}
            assert "from-batch" in target_ids
            assert "from-super-1" in target_ids
            assert "from-super-2" in target_ids


@pytest.mark.asyncio
class TestAdaptiveApocDegreeRouting:

    async def test_very_high_degree_hint_routes_to_apoc(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.ADAPTIVE,
            degree_threshold=200,
            apoc_degree_threshold=1000,
        )
        acl_params = _DEFAULT_ACL
        mock_driver = MagicMock()
        expected = [{"target_id": "svc-apoc"}]

        with patch(
            "orchestrator.app.agentic_traversal._try_apoc_expansion",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock_apoc, patch(
            "orchestrator.app.agentic_traversal._batched_bfs",
            new_callable=AsyncMock,
        ) as mock_bfs:
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
                degree_hint=1500,
            )

            mock_apoc.assert_called_once()
            mock_bfs.assert_not_called()
            assert result == expected

    async def test_apoc_fallback_to_batched_bfs_on_client_error(self) -> None:
        from neo4j.exceptions import ClientError

        config = TraversalConfig(
            strategy=TraversalStrategy.ADAPTIVE,
            degree_threshold=200,
            apoc_degree_threshold=1000,
        )
        acl_params = _DEFAULT_ACL
        mock_driver = MagicMock()
        fallback = [{"target_id": "svc-bfs-fallback"}]

        with patch(
            "orchestrator.app.agentic_traversal._try_apoc_expansion",
            new_callable=AsyncMock,
            side_effect=ClientError("apoc not available"),
        ) as mock_apoc, patch(
            "orchestrator.app.agentic_traversal._batched_bfs",
            new_callable=AsyncMock,
            return_value=fallback,
        ) as mock_bfs:
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
                degree_hint=1500,
            )

            mock_apoc.assert_called_once()
            mock_bfs.assert_called_once()
            assert result == fallback
