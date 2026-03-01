from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.agentic_traversal import (
    TraversalState,
    _BATCHED_NEIGHBOR_TEMPLATE,
    _batched_bfs,
    _drain_frontier,
    execute_batched_hop,
    run_traversal,
)
from orchestrator.app.context_manager import TokenBudget


def _make_mock_driver(query_results: list[list[dict]]) -> AsyncMock:
    call_index = {"i": 0}

    async def _mock_run(query, **params):
        idx = call_index["i"]
        call_index["i"] += 1
        result = AsyncMock()
        data = query_results[idx] if idx < len(query_results) else []
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


async def _stub_low_degrees(driver, source_ids, tenant_id, timeout=30.0):
    return {sid: 5 for sid in source_ids}


_DEFAULT_ACL = {
    "is_admin": True,
    "acl_team": "",
    "acl_namespaces": [],
}


class TestBatchedNeighborTemplate:
    def test_template_uses_unwind(self) -> None:
        assert "UNWIND" in _BATCHED_NEIGHBOR_TEMPLATE
        assert "$frontier_ids" in _BATCHED_NEIGHBOR_TEMPLATE

    def test_template_filters_tenant(self) -> None:
        assert "$tenant_id" in _BATCHED_NEIGHBOR_TEMPLATE

    def test_template_filters_tombstoned(self) -> None:
        assert "tombstoned_at IS NULL" in _BATCHED_NEIGHBOR_TEMPLATE


@pytest.mark.asyncio
class TestExecuteBatchedHop:

    @pytest.fixture(autouse=True)
    def _low_degree_stubs(self):
        with patch(
            "orchestrator.app.agentic_traversal.batch_check_degrees",
            side_effect=_stub_low_degrees,
        ):
            yield

    async def test_single_query_for_multiple_sources(self) -> None:
        hop_results = [
            {"source_id": "a", "target_id": "x", "target_name": "svc-x",
             "rel_type": "CALLS", "target_label": "Service"},
            {"source_id": "b", "target_id": "y", "target_name": "svc-y",
             "rel_type": "CALLS", "target_label": "Service"},
        ]
        driver = _make_mock_driver([hop_results])

        results = await execute_batched_hop(
            driver=driver,
            source_ids=["a", "b"],
            tenant_id="t1",
            acl_params=_DEFAULT_ACL,
        )

        assert len(results) == 2
        assert results[0]["target_id"] == "x"
        assert results[1]["target_id"] == "y"

    async def test_empty_frontier_returns_empty(self) -> None:
        driver = _make_mock_driver([[]])

        results = await execute_batched_hop(
            driver=driver,
            source_ids=[],
            tenant_id="t1",
            acl_params=_DEFAULT_ACL,
        )

        assert results == []


@pytest.mark.asyncio
class TestBatchedBFS:

    @pytest.fixture(autouse=True)
    def _low_degree_stubs(self):
        with patch(
            "orchestrator.app.agentic_traversal.batch_check_degrees",
            side_effect=_stub_low_degrees,
        ):
            yield

    async def test_single_hop_collects_neighbors(self) -> None:
        hop_results = [
            {"source_id": "start", "target_id": "a", "target_name": "svc-a",
             "rel_type": "CALLS", "target_label": "Service"},
        ]
        driver = _make_mock_driver([hop_results, []])

        results = await _batched_bfs(
            driver=driver,
            start_node_id="start",
            tenant_id="t1",
            acl_params=_DEFAULT_ACL,
            max_hops=2,
            timeout=10.0,
            token_budget=TokenBudget(),
        )

        assert len(results) >= 1

    async def test_multi_hop_expands_frontier(self) -> None:
        hop1 = [
            {"source_id": "start", "target_id": "a",
             "target_name": "svc-a", "rel_type": "CALLS",
             "target_label": "Service"},
            {"source_id": "start", "target_id": "b",
             "target_name": "svc-b", "rel_type": "CALLS",
             "target_label": "Service"},
        ]
        hop2 = [
            {"source_id": "a", "target_id": "c",
             "target_name": "svc-c", "rel_type": "CALLS",
             "target_label": "Service"},
        ]
        driver = _make_mock_driver([hop1, hop2, []])

        results = await _batched_bfs(
            driver=driver,
            start_node_id="start",
            tenant_id="t1",
            acl_params=_DEFAULT_ACL,
            max_hops=3,
            timeout=10.0,
            token_budget=TokenBudget(),
        )

        assert len(results) >= 2

    async def test_respects_max_hops(self) -> None:
        hop_data = [
            {"source_id": "start", "target_id": "a",
             "target_name": "svc-a", "rel_type": "CALLS",
             "target_label": "Service"},
        ]
        driver = _make_mock_driver([hop_data] * 10)

        results = await _batched_bfs(
            driver=driver,
            start_node_id="start",
            tenant_id="t1",
            acl_params=_DEFAULT_ACL,
            max_hops=1,
            timeout=10.0,
            token_budget=TokenBudget(),
        )

        assert len(results) >= 1


@pytest.mark.asyncio
class TestRunTraversalFallback:

    @pytest.fixture(autouse=True)
    def _low_degree_stubs(self):
        with patch(
            "orchestrator.app.agentic_traversal.batch_check_degrees",
            side_effect=_stub_low_degrees,
        ):
            yield

    async def test_falls_back_to_batched_bfs(self) -> None:
        from neo4j.exceptions import Neo4jError

        hop_results = [
            {"source_id": "start", "target_id": "n1",
             "target_name": "svc-1", "rel_type": "CALLS",
             "target_label": "Service"},
        ]
        driver = _make_mock_driver([hop_results, []])

        with patch(
            "orchestrator.app.agentic_traversal.bounded_path_expansion",
            side_effect=Neo4jError("timeout"),
        ):
            results = await run_traversal(
                driver=driver,
                start_node_id="start",
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                max_hops=2,
            )

        assert len(results) >= 1


class TestDrainFrontierDedup:
    def test_deduplicates_within_batch(self) -> None:
        state = TraversalState()
        state.frontier = ["A", "B", "A", "C", "B"]

        batch = _drain_frontier(state)

        assert batch == ["A", "B", "C"]
        assert state.frontier == []

    def test_excludes_visited_and_deduplicates(self) -> None:
        state = TraversalState()
        state.frontier = ["A", "B", "A", "C", "B"]
        state.visited_nodes = {"B"}

        batch = _drain_frontier(state)

        assert batch == ["A", "C"]
        assert state.frontier == []
