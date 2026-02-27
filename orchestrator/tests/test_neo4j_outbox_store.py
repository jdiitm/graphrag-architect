from __future__ import annotations

import json
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.vector_sync_outbox import (
    Neo4jOutboxStore,
    VectorSyncEvent,
)


class _MockRecord:
    def __init__(self, row: Dict[str, Any]) -> None:
        self._row = row

    def data(self) -> Dict[str, Any]:
        return self._row


class _MockResult:
    def __init__(self, records: List[Dict[str, Any]]) -> None:
        self._records = [_MockRecord(r) for r in records]

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._records:
            raise StopAsyncIteration
        return self._records.pop(0)


def _make_mock_session(records: List[Dict[str, Any]] | None = None):
    session = AsyncMock()
    tx = AsyncMock()

    mock_result = _MockResult(records if records is not None else [])
    tx.run = AsyncMock(return_value=mock_result)

    async def _execute_write(fn, **kwargs):
        return await fn(tx, **kwargs)

    async def _execute_read(fn, **kwargs):
        return await fn(tx, **kwargs)

    session.execute_write = _execute_write
    session.execute_read = _execute_read
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    return session, tx


def _make_mock_driver(session):
    driver = MagicMock()
    driver.session = MagicMock(return_value=session)
    return driver


@pytest.mark.asyncio
class TestNeo4jOutboxStoreWriteEvent:

    async def test_write_event_executes_create_cypher(self) -> None:
        session, tx = _make_mock_session()
        driver = _make_mock_driver(session)
        store = Neo4jOutboxStore(driver=driver)
        event = VectorSyncEvent(collection="services", pruned_ids=["id-1"])

        await store.write_event(event)

        tx.run.assert_called_once()
        cypher = tx.run.call_args[0][0]
        assert "CREATE" in cypher
        assert "OutboxEvent" in cypher

    async def test_write_event_persists_all_fields(self) -> None:
        session, tx = _make_mock_session()
        driver = _make_mock_driver(session)
        store = Neo4jOutboxStore(driver=driver)
        event = VectorSyncEvent(
            collection="services",
            pruned_ids=["id-1", "id-2"],
            operation="delete",
        )

        await store.write_event(event)

        call_kwargs = tx.run.call_args[1]
        assert call_kwargs["event_id"] == event.event_id
        assert call_kwargs["collection"] == "services"
        assert call_kwargs["operation"] == "delete"
        assert "id-1" in call_kwargs["pruned_ids"]


@pytest.mark.asyncio
class TestNeo4jOutboxStoreLoadPending:

    async def test_load_pending_returns_stored_events(self) -> None:
        records = [{
            "event_id": "evt-1",
            "collection": "services",
            "operation": "delete",
            "pruned_ids": '["id-1"]',
            "vectors": "[]",
            "status": "pending",
            "retry_count": 0,
        }]
        session, tx = _make_mock_session(records)
        driver = _make_mock_driver(session)
        store = Neo4jOutboxStore(driver=driver)

        pending = await store.load_pending()

        assert len(pending) == 1
        assert pending[0].event_id == "evt-1"
        assert pending[0].collection == "services"
        assert pending[0].pruned_ids == ["id-1"]

    async def test_load_pending_returns_empty_when_no_events(self) -> None:
        session, tx = _make_mock_session(records=[])
        driver = _make_mock_driver(session)
        store = Neo4jOutboxStore(driver=driver)

        pending = await store.load_pending()

        assert pending == []


@pytest.mark.asyncio
class TestNeo4jOutboxStoreDeleteEvent:

    async def test_delete_event_removes_by_id(self) -> None:
        session, tx = _make_mock_session()
        driver = _make_mock_driver(session)
        store = Neo4jOutboxStore(driver=driver)

        await store.delete_event("evt-123")

        tx.run.assert_called_once()
        cypher = tx.run.call_args[0][0]
        assert "DELETE" in cypher
        assert "OutboxEvent" in cypher
        assert tx.run.call_args[1]["event_id"] == "evt-123"


@pytest.mark.asyncio
class TestNeo4jOutboxStoreUpdateRetryCount:

    async def test_update_retry_count_sets_field(self) -> None:
        session, tx = _make_mock_session()
        driver = _make_mock_driver(session)
        store = Neo4jOutboxStore(driver=driver)

        await store.update_retry_count("evt-123", 3)

        tx.run.assert_called_once()
        cypher = tx.run.call_args[0][0]
        assert "retry_count" in cypher
        assert tx.run.call_args[1]["event_id"] == "evt-123"
        assert tx.run.call_args[1]["retry_count"] == 3


@pytest.mark.asyncio
class TestNeo4jOutboxStoreProtocolCompliance:

    async def test_implements_outbox_store_protocol(self) -> None:
        from orchestrator.app.vector_sync_outbox import OutboxStore
        session, _ = _make_mock_session()
        driver = _make_mock_driver(session)
        store = Neo4jOutboxStore(driver=driver)
        assert isinstance(store, OutboxStore)


@pytest.mark.asyncio
class TestNeo4jOutboxStoreWriteInTx:

    async def test_write_in_tx_uses_provided_transaction(self) -> None:
        mock_result = _MockResult([])
        tx = AsyncMock()
        tx.run = AsyncMock(return_value=mock_result)

        session, _ = _make_mock_session()
        driver = _make_mock_driver(session)
        store = Neo4jOutboxStore(driver=driver)
        event = VectorSyncEvent(collection="services", pruned_ids=["id-1"])

        await store.write_in_tx(tx, event)

        tx.run.assert_called_once()
        cypher = tx.run.call_args[0][0]
        assert "OutboxEvent" in cypher
        assert tx.run.call_args[1]["event_id"] == event.event_id
