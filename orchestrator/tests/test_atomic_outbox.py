from __future__ import annotations

from typing import Any, List
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from orchestrator.app.extraction_models import ServiceNode
from orchestrator.app.neo4j_client import GraphRepository
from orchestrator.app.vector_sync_outbox import Neo4jOutboxStore, VectorSyncEvent


def _mock_driver_with_tx_capture() -> tuple:
    captured_tx_calls: list = []
    mock_tx = AsyncMock()

    async def _capture_run(query, **kwargs):
        captured_tx_calls.append(("run", query, kwargs))

    mock_tx.run = AsyncMock(side_effect=_capture_run)

    mock_session = AsyncMock()

    async def _execute_write(fn, **kw):
        return await fn(mock_tx, **kw)

    mock_session.execute_write = AsyncMock(side_effect=_execute_write)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_driver = MagicMock()
    mock_driver.session.return_value = mock_session

    return mock_driver, mock_session, mock_tx, captured_tx_calls


SAMPLE_SERVICE = ServiceNode(
    id="order-service",
    name="order-service",
    language="go",
    framework="gin",
    opentelemetry_enabled=True,
    tenant_id="test-tenant",
)


class TestAtomicOutboxWrite:

    @pytest.mark.asyncio
    async def test_commit_topology_accepts_outbox_store(self) -> None:
        driver, _, _, _ = _mock_driver_with_tx_capture()
        outbox_store = AsyncMock(spec=Neo4jOutboxStore)
        repo = GraphRepository(driver, outbox_store=outbox_store)
        assert repo._outbox_store is outbox_store

    @pytest.mark.asyncio
    async def test_commit_topology_writes_outbox_after_graph_tx(self) -> None:
        driver, session, mock_tx, captured = _mock_driver_with_tx_capture()
        outbox_store = AsyncMock(spec=Neo4jOutboxStore)
        repo = GraphRepository(driver, outbox_store=outbox_store)
        event = VectorSyncEvent(
            collection="svc_embeddings",
            pruned_ids=["old-node-1"],
        )

        await repo.commit_topology_with_outbox(
            entities=[SAMPLE_SERVICE],
            outbox_events=[event],
        )

        outbox_store.write_in_tx.assert_not_called()
        outbox_store.write_after_tx.assert_called_once_with([event])

    @pytest.mark.asyncio
    async def test_commit_without_events_skips_outbox_write(self) -> None:
        driver, _, _, _ = _mock_driver_with_tx_capture()
        outbox_store = AsyncMock(spec=Neo4jOutboxStore)
        repo = GraphRepository(driver, outbox_store=outbox_store)

        await repo.commit_topology_with_outbox(
            entities=[SAMPLE_SERVICE],
            outbox_events=[],
        )

        outbox_store.write_after_tx.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_outbox_store_still_commits_entities(self) -> None:
        driver, _, mock_tx, _ = _mock_driver_with_tx_capture()
        repo = GraphRepository(driver)

        await repo.commit_topology_with_outbox(
            entities=[SAMPLE_SERVICE],
            outbox_events=[],
        )

        assert mock_tx.run.call_count >= 1
        query_arg = mock_tx.run.call_args_list[0].args[0]
        assert "UNWIND" in query_arg

    @pytest.mark.asyncio
    async def test_topology_committed_even_if_outbox_write_after_tx_fails(
        self,
    ) -> None:
        driver, session, mock_tx, captured = _mock_driver_with_tx_capture()
        outbox_store = AsyncMock(spec=Neo4jOutboxStore)
        outbox_store.write_after_tx = AsyncMock(
            side_effect=RuntimeError("outbox down"),
        )
        repo = GraphRepository(driver, outbox_store=outbox_store)
        event = VectorSyncEvent(
            collection="svc_embeddings", pruned_ids=["old-node-1"],
        )

        with pytest.raises(RuntimeError, match="outbox down"):
            await repo.commit_topology_with_outbox(
                entities=[SAMPLE_SERVICE],
                outbox_events=[event],
            )

        assert mock_tx.run.call_count >= 1
        query_arg = mock_tx.run.call_args_list[0].args[0]
        assert "UNWIND" in query_arg
