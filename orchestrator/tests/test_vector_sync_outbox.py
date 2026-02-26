from __future__ import annotations

import asyncio
from typing import Any, Dict, List
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.vector_sync_outbox import (
    OutboxDrainer,
    VectorSyncEvent,
    VectorSyncOutbox,
)


class TestVectorSyncEvent:

    def test_event_requires_collection_and_ids(self) -> None:
        event = VectorSyncEvent(
            collection="services", pruned_ids=["id-1", "id-2"],
        )
        assert event.collection == "services"
        assert event.pruned_ids == ["id-1", "id-2"]

    def test_event_has_pending_status_by_default(self) -> None:
        event = VectorSyncEvent(
            collection="services", pruned_ids=["id-1"],
        )
        assert event.status == "pending"

    def test_event_has_unique_id(self) -> None:
        a = VectorSyncEvent(collection="x", pruned_ids=["a"])
        b = VectorSyncEvent(collection="x", pruned_ids=["a"])
        assert a.event_id != b.event_id

    def test_event_rejects_empty_pruned_ids(self) -> None:
        with pytest.raises(ValueError):
            VectorSyncEvent(collection="services", pruned_ids=[])


class TestVectorSyncOutbox:

    def test_enqueue_adds_event(self) -> None:
        outbox = VectorSyncOutbox()
        event = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        outbox.enqueue(event)
        assert outbox.pending_count == 1

    def test_drain_returns_pending_events(self) -> None:
        outbox = VectorSyncOutbox()
        e1 = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        e2 = VectorSyncEvent(collection="svc", pruned_ids=["b"])
        outbox.enqueue(e1)
        outbox.enqueue(e2)
        pending = outbox.drain_pending()
        assert len(pending) == 2

    def test_mark_emitted_removes_from_pending(self) -> None:
        outbox = VectorSyncOutbox()
        event = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        outbox.enqueue(event)
        outbox.mark_emitted(event.event_id)
        assert outbox.pending_count == 0

    def test_mark_emitted_unknown_id_is_noop(self) -> None:
        outbox = VectorSyncOutbox()
        outbox.mark_emitted("nonexistent-id")
        assert outbox.pending_count == 0

    def test_drain_pending_preserves_order(self) -> None:
        outbox = VectorSyncOutbox()
        ids_in = []
        for i in range(5):
            event = VectorSyncEvent(collection="svc", pruned_ids=[f"v{i}"])
            outbox.enqueue(event)
            ids_in.append(event.event_id)
        drained = outbox.drain_pending()
        assert [e.event_id for e in drained] == ids_in

    def test_enqueue_after_drain_shows_only_new(self) -> None:
        outbox = VectorSyncOutbox()
        e1 = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        outbox.enqueue(e1)
        outbox.mark_emitted(e1.event_id)

        e2 = VectorSyncEvent(collection="svc", pruned_ids=["b"])
        outbox.enqueue(e2)
        pending = outbox.drain_pending()
        assert len(pending) == 1
        assert pending[0].event_id == e2.event_id


class TestOutboxDrainer:

    @pytest.mark.asyncio
    async def test_drainer_processes_pending_events(self) -> None:
        outbox = VectorSyncOutbox()
        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(return_value=2)

        event = VectorSyncEvent(
            collection="services", pruned_ids=["id-1", "id-2"],
        )
        outbox.enqueue(event)

        drainer = OutboxDrainer(outbox=outbox, vector_store=mock_vs)
        processed = await drainer.process_once()

        assert processed == 1
        mock_vs.delete.assert_called_once_with("services", ["id-1", "id-2"])
        assert outbox.pending_count == 0

    @pytest.mark.asyncio
    async def test_drainer_retains_event_on_delete_failure(self) -> None:
        outbox = VectorSyncOutbox()
        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(side_effect=RuntimeError("Qdrant down"))

        event = VectorSyncEvent(
            collection="services", pruned_ids=["id-1"],
        )
        outbox.enqueue(event)

        drainer = OutboxDrainer(outbox=outbox, vector_store=mock_vs)
        processed = await drainer.process_once()

        assert processed == 0
        assert outbox.pending_count == 1

    @pytest.mark.asyncio
    async def test_drainer_idempotent_on_nonexistent_vectors(self) -> None:
        outbox = VectorSyncOutbox()
        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(return_value=0)

        event = VectorSyncEvent(
            collection="services", pruned_ids=["gone-already"],
        )
        outbox.enqueue(event)

        drainer = OutboxDrainer(outbox=outbox, vector_store=mock_vs)
        processed = await drainer.process_once()

        assert processed == 1
        assert outbox.pending_count == 0

    @pytest.mark.asyncio
    async def test_drainer_processes_multiple_events(self) -> None:
        outbox = VectorSyncOutbox()
        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(return_value=1)

        for i in range(3):
            outbox.enqueue(
                VectorSyncEvent(collection="svc", pruned_ids=[f"v{i}"]),
            )

        drainer = OutboxDrainer(outbox=outbox, vector_store=mock_vs)
        processed = await drainer.process_once()

        assert processed == 3
        assert outbox.pending_count == 0
        assert mock_vs.delete.call_count == 3

    @pytest.mark.asyncio
    async def test_drainer_partial_failure_retains_failed(self) -> None:
        outbox = VectorSyncOutbox()
        call_count = 0

        async def _alternating_delete(collection: str, ids: list) -> int:
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("transient failure")
            return len(ids)

        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(side_effect=_alternating_delete)

        events = []
        for i in range(3):
            evt = VectorSyncEvent(collection="svc", pruned_ids=[f"v{i}"])
            outbox.enqueue(evt)
            events.append(evt)

        drainer = OutboxDrainer(outbox=outbox, vector_store=mock_vs)
        processed = await drainer.process_once()

        assert processed == 2
        assert outbox.pending_count == 1

    @pytest.mark.asyncio
    async def test_drainer_noop_when_outbox_empty(self) -> None:
        outbox = VectorSyncOutbox()
        mock_vs = AsyncMock()

        drainer = OutboxDrainer(outbox=outbox, vector_store=mock_vs)
        processed = await drainer.process_once()

        assert processed == 0
        mock_vs.delete.assert_not_called()
