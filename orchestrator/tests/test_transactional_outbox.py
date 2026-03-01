from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.vector_sync_outbox import (
    Neo4jOutboxStore,
    VectorSyncEvent,
)


class _FakeTx:

    def __init__(self) -> None:
        self.runs: list[tuple[str, dict[str, Any]]] = []

    async def run(self, query: str, **kwargs: Any) -> None:
        self.runs.append((query, kwargs))


class _FakeSession:

    def __init__(self) -> None:
        self.execute_write = AsyncMock()
        self.execute_read = AsyncMock(return_value=[])

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass


def _make_driver(session: _FakeSession | None = None) -> MagicMock:
    s = session or _FakeSession()
    driver = MagicMock()
    driver.session.return_value = s
    return driver


class TestNeo4jOutboxStoreWriteEvent:

    @pytest.mark.asyncio
    async def test_write_event_creates_outbox_node(self) -> None:
        session = _FakeSession()
        driver = _make_driver(session)

        store = Neo4jOutboxStore(driver=driver)
        event = VectorSyncEvent(collection="services", pruned_ids=["id-1", "id-2"])
        await store.write_event(event)

        session.execute_write.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_write_event_cypher_contains_create(self) -> None:
        session = _FakeSession()
        captured_args: list[Any] = []

        async def _capture_write(fn, **kwargs):
            tx = _FakeTx()
            await fn(tx, **kwargs)
            captured_args.extend(tx.runs)

        session.execute_write = AsyncMock(side_effect=_capture_write)
        driver = _make_driver(session)

        store = Neo4jOutboxStore(driver=driver)
        event = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        await store.write_event(event)

        assert len(captured_args) == 1
        query, params = captured_args[0]
        assert "CREATE" in query.upper() or "MERGE" in query.upper()
        assert "OutboxEvent" in query
        assert params["event_id"] == event.event_id


class TestNeo4jOutboxStoreWriteInTx:

    @pytest.mark.asyncio
    async def test_write_in_tx_uses_provided_transaction(self) -> None:
        driver = AsyncMock()
        store = Neo4jOutboxStore(driver=driver)
        event = VectorSyncEvent(collection="svc", pruned_ids=["x"])

        tx = _FakeTx()
        await store.write_in_tx(tx, event)

        assert len(tx.runs) == 1
        query, params = tx.runs[0]
        assert "OutboxEvent" in query
        assert params["event_id"] == event.event_id
        assert params["collection"] == "svc"

    @pytest.mark.asyncio
    async def test_write_in_tx_serializes_pruned_ids(self) -> None:
        driver = AsyncMock()
        store = Neo4jOutboxStore(driver=driver)
        event = VectorSyncEvent(collection="svc", pruned_ids=["a", "b", "c"])

        tx = _FakeTx()
        await store.write_in_tx(tx, event)

        _, params = tx.runs[0]
        assert json.loads(params["pruned_ids"]) == ["a", "b", "c"]


class TestNeo4jOutboxStoreLoadPending:

    @pytest.mark.asyncio
    async def test_load_pending_returns_stored_events(self) -> None:
        session = _FakeSession()
        event_data = {
            "event_id": "evt-1",
            "collection": "services",
            "operation": "delete",
            "pruned_ids": '["id-1"]',
            "vectors": "[]",
            "status": "pending",
            "retry_count": 0,
        }

        async def _run_read(fn, **kwargs):
            return [event_data]

        session.execute_read = AsyncMock(side_effect=_run_read)
        driver = _make_driver(session)

        store = Neo4jOutboxStore(driver=driver)
        pending = await store.load_pending()

        assert len(pending) == 1
        assert pending[0].event_id == "evt-1"
        assert pending[0].collection == "services"
        assert pending[0].pruned_ids == ["id-1"]

    @pytest.mark.asyncio
    async def test_load_pending_returns_empty_when_no_events(self) -> None:
        session = _FakeSession()

        async def _run_read(fn, **kwargs):
            return []

        session.execute_read = AsyncMock(side_effect=_run_read)
        driver = _make_driver(session)

        store = Neo4jOutboxStore(driver=driver)
        pending = await store.load_pending()

        assert pending == []


class TestNeo4jOutboxStoreDeleteEvent:

    @pytest.mark.asyncio
    async def test_delete_event_removes_outbox_node(self) -> None:
        session = _FakeSession()
        captured_args: list[Any] = []

        async def _capture_write(fn, **kwargs):
            tx = _FakeTx()
            await fn(tx, **kwargs)
            captured_args.extend(tx.runs)

        session.execute_write = AsyncMock(side_effect=_capture_write)
        driver = _make_driver(session)

        store = Neo4jOutboxStore(driver=driver)
        await store.delete_event("evt-123")

        assert len(captured_args) == 1
        query, params = captured_args[0]
        assert "DELETE" in query.upper()
        assert params["event_id"] == "evt-123"


class TestNeo4jOutboxStoreUpdateRetryCount:

    @pytest.mark.asyncio
    async def test_update_retry_count_sets_field(self) -> None:
        session = _FakeSession()
        captured_args: list[Any] = []

        async def _capture_write(fn, **kwargs):
            tx = _FakeTx()
            await fn(tx, **kwargs)
            captured_args.extend(tx.runs)

        session.execute_write = AsyncMock(side_effect=_capture_write)
        driver = _make_driver(session)

        store = Neo4jOutboxStore(driver=driver)
        await store.update_retry_count("evt-123", 3)

        assert len(captured_args) == 1
        query, params = captured_args[0]
        assert "retry_count" in query
        assert params["retry_count"] == 3
        assert params["event_id"] == "evt-123"


class TestNeo4jOutboxStoreProtocolCompliance:

    def test_neo4j_store_satisfies_outbox_store_protocol(self) -> None:
        from orchestrator.app.vector_sync_outbox import OutboxStore
        driver = AsyncMock()
        store = Neo4jOutboxStore(driver=driver)
        assert isinstance(store, OutboxStore)


class TestNeo4jOutboxStoreWriteAfterTx:

    @pytest.mark.asyncio
    async def test_write_after_tx_opens_own_session(self) -> None:
        session = _FakeSession()
        driver = _make_driver(session)
        store = Neo4jOutboxStore(driver=driver)
        event = VectorSyncEvent(collection="svc", pruned_ids=["a"])

        await store.write_after_tx([event])

        session.execute_write.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_write_after_tx_writes_all_events(self) -> None:
        session = _FakeSession()
        captured_args: list[Any] = []

        async def _capture_write(fn: Any, **kwargs: Any) -> None:
            tx = _FakeTx()
            await fn(tx, **kwargs)
            captured_args.extend(tx.runs)

        session.execute_write = AsyncMock(side_effect=_capture_write)
        driver = _make_driver(session)
        store = Neo4jOutboxStore(driver=driver)
        events = [
            VectorSyncEvent(collection="svc", pruned_ids=["a"]),
            VectorSyncEvent(collection="svc", pruned_ids=["b"]),
        ]

        await store.write_after_tx(events)

        assert len(captured_args) == 2
        for query, params in captured_args:
            assert "OutboxEvent" in query
            assert "CREATE" in query

    @pytest.mark.asyncio
    async def test_write_after_tx_noop_for_empty_list(self) -> None:
        session = _FakeSession()
        driver = _make_driver(session)
        store = Neo4jOutboxStore(driver=driver)

        await store.write_after_tx([])

        session.execute_write.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_write_after_tx_does_not_use_caller_tx(self) -> None:
        session = _FakeSession()
        caller_tx = _FakeTx()
        driver = _make_driver(session)
        store = Neo4jOutboxStore(driver=driver)
        event = VectorSyncEvent(collection="svc", pruned_ids=["a"])

        await store.write_after_tx([event])

        assert len(caller_tx.runs) == 0
