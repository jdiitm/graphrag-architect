from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.agentic_traversal import (
    TraversalAgent,
    TraversalState,
    TraversalStep,
    _NEIGHBOR_DISCOVERY_TEMPLATE,
    build_one_hop_cypher,
    execute_hop,
    run_traversal,
)
from orchestrator.app.context_manager import TokenBudget


_LOW_DEGREE = [{"degree": 5}]


def _mock_neo4j_session(side_effect=None, return_value=None):
    mock_session = AsyncMock()
    if side_effect:
        degree_interleaved = []
        for item in side_effect:
            degree_interleaved.append(_LOW_DEGREE)
            degree_interleaved.append(item)
        mock_session.execute_read = AsyncMock(side_effect=degree_interleaved)
    else:
        data = return_value if return_value is not None else []
        mock_session.execute_read = AsyncMock(side_effect=[_LOW_DEGREE, data])
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    return mock_session


def _mock_driver(session):
    driver = MagicMock()
    driver.session.return_value = session
    driver.close = AsyncMock()
    return driver


class TestExecuteHop:
    @pytest.mark.asyncio
    async def test_returns_neighbors_for_valid_node(self) -> None:
        neo4j_records = [
            {
                "target_id": "svc-b",
                "target_name": "payment-service",
                "rel_type": "CALLS",
                "target_label": "Service",
            },
        ]
        session = _mock_neo4j_session(return_value=neo4j_records)
        driver = _mock_driver(session)

        results = await execute_hop(
            driver=driver,
            source_id="svc-a",
            tenant_id="tenant-1",
            acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
            timeout=30.0,
        )

        assert len(results) == 1
        assert results[0]["target_id"] == "svc-b"
        assert session.execute_read.call_count == 2

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_neighbors(self) -> None:
        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(
            side_effect=[_LOW_DEGREE, []]
        )
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        driver = _mock_driver(mock_session)

        results = await execute_hop(
            driver=driver,
            source_id="isolated-node",
            tenant_id="tenant-1",
            acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
            timeout=30.0,
        )

        assert results == []

    @pytest.mark.asyncio
    async def test_passes_tenant_and_acl_params(self) -> None:
        session = _mock_neo4j_session(return_value=[])
        driver = _mock_driver(session)

        await execute_hop(
            driver=driver,
            source_id="svc-a",
            tenant_id="t-42",
            acl_params={"is_admin": False, "acl_team": "platform", "acl_namespaces": ["ns1"]},
            timeout=30.0,
        )

        assert session.execute_read.call_count == 2


class TestRunTraversal:
    @pytest.fixture(autouse=True)
    def _force_sequential_fallback(self):
        with patch(
            "orchestrator.app.agentic_traversal.bounded_path_expansion",
            new_callable=AsyncMock,
            side_effect=asyncio.TimeoutError("force sequential BFS in legacy tests"),
        ):
            yield

    @pytest.mark.asyncio
    async def test_single_hop_traversal(self) -> None:
        from neo4j.exceptions import Neo4jError

        batched_results = [
            {
                "source_id": "svc-a",
                "target_id": "svc-b",
                "target_name": "payment-service",
                "rel_type": "CALLS",
                "target_label": "Service",
            },
        ]
        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(
            side_effect=[batched_results, []]
        )
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        driver = _mock_driver(mock_session)

        with patch(
            "orchestrator.app.agentic_traversal.bounded_path_expansion",
            new_callable=AsyncMock,
            side_effect=Neo4jError("force fallback"),
        ):
            context = await run_traversal(
                driver=driver,
                start_node_id="svc-a",
                tenant_id="tenant-1",
                acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
                max_hops=2,
                timeout=30.0,
            )

        assert len(context) >= 1
        assert any(r.get("target_id") == "svc-b" for r in context)

    @pytest.mark.asyncio
    async def test_traversal_stops_at_max_hops(self) -> None:
        from neo4j.exceptions import Neo4jError

        hop1_results = [
            {"source_id": "root", "target_id": f"svc-{i}",
             "target_name": f"service-{i}", "rel_type": "CALLS",
             "target_label": "Service"}
            for i in range(5)
        ]
        hop2_results = [
            {"source_id": f"svc-{i}", "target_id": f"deep-{i}",
             "target_name": f"deep-{i}", "rel_type": "CALLS",
             "target_label": "Service"}
            for i in range(5)
        ]
        hop3_results = [
            {"source_id": f"deep-{i}", "target_id": f"ultra-{i}",
             "target_name": f"ultra-{i}", "rel_type": "CALLS",
             "target_label": "Service"}
            for i in range(5)
        ]
        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(
            side_effect=[hop1_results, hop2_results, hop3_results]
        )
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        driver = _mock_driver(mock_session)

        with patch(
            "orchestrator.app.agentic_traversal.bounded_path_expansion",
            new_callable=AsyncMock,
            side_effect=Neo4jError("force fallback"),
        ):
            context = await run_traversal(
                driver=driver,
                start_node_id="root",
                tenant_id="t1",
                acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
                max_hops=2,
                timeout=30.0,
            )

        assert mock_session.execute_read.call_count == 2
        assert len(context) >= 1

    @pytest.mark.asyncio
    async def test_traversal_respects_token_budget(self) -> None:
        from neo4j.exceptions import Neo4jError

        hop1_results = [
            {"source_id": "root", "target_id": f"svc-{i}",
             "target_name": f"service-{i}", "rel_type": "CALLS",
             "target_label": "Service"}
            for i in range(3)
        ]
        hop2_results = [
            {"source_id": f"svc-{i}", "target_id": f"deep-{i}",
             "target_name": f"deep-{i}", "rel_type": "CALLS",
             "target_label": "Service"}
            for i in range(3)
        ]
        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(
            side_effect=[hop1_results, hop2_results, []]
        )
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        driver = _mock_driver(mock_session)

        with patch(
            "orchestrator.app.agentic_traversal.bounded_path_expansion",
            new_callable=AsyncMock,
            side_effect=Neo4jError("force fallback"),
        ), patch(
            "orchestrator.app.agentic_traversal.estimate_tokens",
            return_value=10,
        ), patch(
            "orchestrator.app.context_manager.estimate_tokens",
            return_value=10,
        ):
            context = await run_traversal(
                driver=driver,
                start_node_id="root",
                tenant_id="t1",
                acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
                max_hops=5,
                timeout=30.0,
                token_budget=TokenBudget(max_context_tokens=30),
            )

        assert mock_session.execute_read.call_count == 1
        assert len(context) == 3

    @pytest.mark.asyncio
    async def test_traversal_does_not_revisit_nodes(self) -> None:
        from neo4j.exceptions import Neo4jError

        cycle_results = [
            {
                "source_id": "svc-a",
                "target_id": "svc-a",
                "target_name": "self-ref",
                "rel_type": "CALLS",
                "target_label": "Service",
            },
        ]
        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(
            side_effect=[cycle_results, []]
        )
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        driver = _mock_driver(mock_session)

        with patch(
            "orchestrator.app.agentic_traversal.bounded_path_expansion",
            new_callable=AsyncMock,
            side_effect=Neo4jError("force fallback"),
        ):
            await run_traversal(
                driver=driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
                max_hops=5,
                timeout=30.0,
            )

        assert mock_session.execute_read.call_count <= 2

    @pytest.mark.asyncio
    async def test_traversal_returns_empty_for_isolated_node(self) -> None:
        from neo4j.exceptions import Neo4jError

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(side_effect=[[]])
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        driver = _mock_driver(mock_session)

        with patch(
            "orchestrator.app.agentic_traversal.bounded_path_expansion",
            new_callable=AsyncMock,
            side_effect=Neo4jError("force fallback"),
        ):
            context = await run_traversal(
                driver=driver,
                start_node_id="orphan",
                tenant_id="t1",
                acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
                max_hops=3,
                timeout=30.0,
            )

        assert context == []
        assert mock_session.execute_read.call_count == 1
