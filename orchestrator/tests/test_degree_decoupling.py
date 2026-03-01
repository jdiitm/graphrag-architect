from __future__ import annotations

from typing import Any, List
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from orchestrator.app.extraction_models import (
    CallsEdge,
    ServiceNode,
)
from orchestrator.app.neo4j_client import GraphRepository


SAMPLE_SERVICE_A = ServiceNode(
    id="order-service",
    name="order-service",
    language="go",
    framework="gin",
    opentelemetry_enabled=True,
    tenant_id="test-tenant",
)

SAMPLE_SERVICE_B = ServiceNode(
    id="user-service",
    name="user-service",
    language="python",
    framework="fastapi",
    opentelemetry_enabled=False,
    tenant_id="test-tenant",
)

SAMPLE_CALLS = CallsEdge(
    source_service_id="user-service",
    target_service_id="order-service",
    protocol="http",
    tenant_id="test-tenant",
)


def _mock_driver() -> tuple:
    mock_tx = AsyncMock()
    mock_tx.run = AsyncMock()

    mock_session = AsyncMock()

    async def _execute_write_side_effect(fn, **kw):
        return await fn(mock_tx, **kw)

    mock_session.execute_write = AsyncMock(side_effect=_execute_write_side_effect)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_driver = MagicMock()
    mock_driver.session.return_value = mock_session

    return mock_driver, mock_session, mock_tx


class TestDegreeRefreshDecoupled:

    @pytest.mark.asyncio
    async def test_commit_topology_does_not_run_degree_refresh(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)

        await repo.commit_topology([SAMPLE_SERVICE_A, SAMPLE_SERVICE_B, SAMPLE_CALLS])

        all_cypher = [c.args[0] for c in tx.run.call_args_list]
        degree_calls = [q for q in all_cypher if "n.degree" in q]
        assert len(degree_calls) == 0, (
            f"commit_topology must NOT run degree refresh synchronously; "
            f"found degree calls: {degree_calls}"
        )

    @pytest.mark.asyncio
    async def test_commit_topology_still_writes_nodes_and_edges(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)

        await repo.commit_topology([SAMPLE_SERVICE_A, SAMPLE_SERVICE_B, SAMPLE_CALLS])

        all_cypher = [c.args[0] for c in tx.run.call_args_list]
        unwind_writes = [q for q in all_cypher if "UNWIND" in q]
        degree_calls = [q for q in all_cypher if "n.degree" in q]
        assert len(unwind_writes) >= 2
        assert len(degree_calls) == 0, (
            f"commit_topology should NOT include degree refresh; "
            f"found: {degree_calls}"
        )
        assert len(all_cypher) == len(unwind_writes), (
            f"commit_topology should ONLY produce UNWIND write operations; "
            f"got {len(all_cypher)} total calls but "
            f"{len(unwind_writes)} were UNWIND writes"
        )

    @pytest.mark.asyncio
    async def test_refresh_degree_available_as_standalone_method(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)

        await repo.refresh_degree_for_ids(
            ["order-service", "user-service"], tenant_id="test-tenant",
        )

        all_cypher = [c.args[0] for c in tx.run.call_args_list]
        degree_calls = [q for q in all_cypher if "n.degree" in q]
        assert len(degree_calls) == 1

    @pytest.mark.asyncio
    async def test_refresh_degree_noop_on_empty_ids(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)

        await repo.refresh_degree_for_ids([])

        tx.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_commit_returns_affected_ids(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)

        affected = await repo.commit_topology_with_affected_ids(
            [SAMPLE_SERVICE_A, SAMPLE_CALLS],
        )

        assert isinstance(affected, list)
        assert "order-service" in affected
        assert "user-service" in affected
