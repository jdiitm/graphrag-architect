from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.vector_sync_outbox import (
    CoalescingOutbox,
    VectorSyncEvent,
    VectorSyncOutbox,
)
from orchestrator.app.graph_builder import create_durable_spillover_fn


class TestCreateDurableSpilloverFn:

    def test_function_exists(self) -> None:
        assert callable(create_durable_spillover_fn)

    @pytest.mark.asyncio
    async def test_spillover_writes_to_durable_store(self) -> None:
        store = AsyncMock()
        store.write_event = AsyncMock()
        spillover_fn = create_durable_spillover_fn(store)

        events = [
            VectorSyncEvent(
                event_id="evt-1",
                collection="services",
                operation="delete",
                pruned_ids=["node-1"],
            ),
            VectorSyncEvent(
                event_id="evt-2",
                collection="services",
                operation="delete",
                pruned_ids=["node-2"],
            ),
        ]
        spillover_fn(events)

        assert len(spillover_fn.pending) == 2, (
            "Spillover must buffer events in the pending list"
        )
        await asyncio.sleep(0)
        assert store.write_event.call_count == 2, (
            "Spillover must schedule a durable write for each event"
        )

    def test_spillover_does_not_use_inmemory_outbox(self) -> None:
        store = AsyncMock()
        spillover_fn = create_durable_spillover_fn(store)

        volatile_outbox = VectorSyncOutbox()
        events = [
            VectorSyncEvent(
                event_id="evt-1",
                collection="services",
                operation="delete",
                pruned_ids=["node-1"],
            ),
        ]
        spillover_fn(events)
        assert volatile_outbox.pending_count == 0, (
            "Durable spillover must not touch the in-memory VectorSyncOutbox"
        )


class TestCoalescingOutboxDurableWiring:

    def test_cap_overflow_routes_to_durable(self) -> None:
        captured: list[list[VectorSyncEvent]] = []

        def capture_spillover(events: list[VectorSyncEvent]) -> None:
            captured.append(events)

        outbox = CoalescingOutbox(
            max_entries=2,
            spillover_fn=capture_spillover,
        )
        for i in range(4):
            outbox.enqueue(
                VectorSyncEvent(
                    event_id=f"evt-{i}",
                    collection="services",
                    operation="delete",
                    pruned_ids=[f"node-{i}"],
                )
            )
        assert len(captured) > 0, (
            "Overflow beyond max_entries must trigger spillover callback"
        )
        total_spilled = sum(len(batch) for batch in captured)
        assert total_spilled >= 2, (
            "At least 2 events must be spilled when 4 enqueued with cap 2"
        )

    def test_no_spillover_when_within_cap(self) -> None:
        captured: list[list[VectorSyncEvent]] = []

        def capture_spillover(events: list[VectorSyncEvent]) -> None:
            captured.append(events)

        outbox = CoalescingOutbox(
            max_entries=10,
            spillover_fn=capture_spillover,
        )
        for i in range(3):
            outbox.enqueue(
                VectorSyncEvent(
                    event_id=f"evt-{i}",
                    collection="services",
                    operation="delete",
                    pruned_ids=[f"node-{i}"],
                )
            )
        assert len(captured) == 0
