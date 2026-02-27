from __future__ import annotations

from typing import List
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.vector_sync_outbox import (
    DurableOutboxDrainer,
    OutboxDrainer,
    VectorSyncEvent,
    VectorSyncOutbox,
)


class _FakeOutboxStore:

    def __init__(self) -> None:
        self._events: dict[str, VectorSyncEvent] = {}

    async def write_event(self, event: VectorSyncEvent) -> None:
        self._events[event.event_id] = event

    async def load_pending(self) -> List[VectorSyncEvent]:
        return list(self._events.values())

    async def delete_event(self, event_id: str) -> None:
        self._events.pop(event_id, None)

    async def update_retry_count(
        self, event_id: str, retry_count: int,
    ) -> None:
        if event_id in self._events:
            self._events[event_id] = self._events[event_id].model_copy(
                update={"retry_count": retry_count},
            )


class TestDrainVectorOutboxPriority:

    @pytest.mark.asyncio
    async def test_durable_events_drained_even_when_inmemory_has_pending(
        self,
    ) -> None:
        durable_store = _FakeOutboxStore()
        durable_event = VectorSyncEvent(
            collection="svc", pruned_ids=["durable-1"],
        )
        await durable_store.write_event(durable_event)

        inmemory_outbox = VectorSyncOutbox()
        inmemory_event = VectorSyncEvent(
            collection="svc", pruned_ids=["inmem-1"],
        )
        inmemory_outbox.enqueue(inmemory_event)

        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(return_value=1)

        with (
            patch(
                "orchestrator.app.graph_builder._VECTOR_OUTBOX",
                inmemory_outbox,
            ),
            patch(
                "orchestrator.app.graph_builder.get_vector_store",
                return_value=mock_vs,
            ),
            patch(
                "orchestrator.app.graph_builder._get_redis_conn",
                return_value=None,
            ),
            patch(
                "orchestrator.app.graph_builder.get_driver",
                return_value=AsyncMock(),
            ),
            patch(
                "orchestrator.app.graph_builder.create_outbox_drainer",
                return_value=DurableOutboxDrainer(
                    store=durable_store, vector_store=mock_vs,
                ),
            ),
        ):
            from orchestrator.app.graph_builder import drain_vector_outbox
            total = await drain_vector_outbox()

        assert total >= 1, "Durable events must be drained"
        durable_remaining = await durable_store.load_pending()
        assert len(durable_remaining) == 0, (
            "Durable store must be empty after drain"
        )
        assert inmemory_outbox.pending_count == 0, (
            "In-memory outbox must also be drained"
        )

    @pytest.mark.asyncio
    async def test_durable_drained_first_before_inmemory(self) -> None:
        drain_order: list[str] = []

        durable_store = _FakeOutboxStore()
        durable_event = VectorSyncEvent(
            collection="svc", pruned_ids=["durable-1"],
        )
        await durable_store.write_event(durable_event)

        inmemory_outbox = VectorSyncOutbox()
        inmemory_event = VectorSyncEvent(
            collection="svc", pruned_ids=["inmem-1"],
        )
        inmemory_outbox.enqueue(inmemory_event)

        async def _tracking_delete(collection: str, ids: list) -> int:
            drain_order.append(ids[0])
            return len(ids)

        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(side_effect=_tracking_delete)

        with (
            patch(
                "orchestrator.app.graph_builder._VECTOR_OUTBOX",
                inmemory_outbox,
            ),
            patch(
                "orchestrator.app.graph_builder.get_vector_store",
                return_value=mock_vs,
            ),
            patch(
                "orchestrator.app.graph_builder._get_redis_conn",
                return_value=None,
            ),
            patch(
                "orchestrator.app.graph_builder.get_driver",
                return_value=AsyncMock(),
            ),
            patch(
                "orchestrator.app.graph_builder.create_outbox_drainer",
                return_value=DurableOutboxDrainer(
                    store=durable_store, vector_store=mock_vs,
                ),
            ),
        ):
            from orchestrator.app.graph_builder import drain_vector_outbox
            await drain_vector_outbox()

        assert len(drain_order) == 2
        assert drain_order[0] == "durable-1", (
            "Durable must drain before in-memory"
        )
        assert drain_order[1] == "inmem-1", (
            "In-memory must drain after durable"
        )

    @pytest.mark.asyncio
    async def test_pod_restart_only_durable_events_remain(self) -> None:
        durable_store = _FakeOutboxStore()
        durable_event = VectorSyncEvent(
            collection="svc", pruned_ids=["survived-restart"],
        )
        await durable_store.write_event(durable_event)

        fresh_outbox = VectorSyncOutbox()
        assert fresh_outbox.pending_count == 0

        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(return_value=1)

        with (
            patch(
                "orchestrator.app.graph_builder._VECTOR_OUTBOX",
                fresh_outbox,
            ),
            patch(
                "orchestrator.app.graph_builder.get_vector_store",
                return_value=mock_vs,
            ),
            patch(
                "orchestrator.app.graph_builder._get_redis_conn",
                return_value=None,
            ),
            patch(
                "orchestrator.app.graph_builder.get_driver",
                return_value=AsyncMock(),
            ),
            patch(
                "orchestrator.app.graph_builder.create_outbox_drainer",
                return_value=DurableOutboxDrainer(
                    store=durable_store, vector_store=mock_vs,
                ),
            ),
        ):
            from orchestrator.app.graph_builder import drain_vector_outbox
            total = await drain_vector_outbox()

        assert total == 1
        remaining = await durable_store.load_pending()
        assert len(remaining) == 0
        mock_vs.delete.assert_called_once_with("svc", ["survived-restart"])
