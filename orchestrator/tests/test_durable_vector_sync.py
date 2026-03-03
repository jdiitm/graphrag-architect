from __future__ import annotations

from typing import List
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.vector_sync_outbox import (
    DurableOutboxDrainer,
    OutboxStore,
    VectorSyncEvent,
)


class TestVectorSyncEventRetryCount:

    def test_event_default_retry_count_zero(self) -> None:
        event = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        assert event.retry_count == 0

    def test_event_accepts_explicit_retry_count(self) -> None:
        event = VectorSyncEvent(
            collection="svc", pruned_ids=["a"], retry_count=3,
        )
        assert event.retry_count == 3


class _FakeOutboxStore:

    def __init__(self) -> None:
        self._events: dict[str, VectorSyncEvent] = {}
        self._fences: dict[str, int] = {}

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

    async def get_version_fence(self, fence_key: str, tenant_id: str = "") -> int:
        key = f"{tenant_id}:{fence_key}"
        return self._fences.get(key, -1)

    async def update_version_fence(
        self, fence_key: str, version: int, tenant_id: str = "",
    ) -> None:
        key = f"{tenant_id}:{fence_key}"
        current = self._fences.get(key, -1)
        if version > current:
            self._fences[key] = version


class TestOutboxStoreProtocol:

    def test_fake_store_satisfies_protocol(self) -> None:
        store = _FakeOutboxStore()
        assert isinstance(store, OutboxStore)


class TestDurableOutboxDrainer:

    @pytest.mark.asyncio
    async def test_processes_pending_from_store(self) -> None:
        store = _FakeOutboxStore()
        event = VectorSyncEvent(collection="svc", pruned_ids=["id-1"])
        await store.write_event(event)

        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(return_value=1)

        drainer = DurableOutboxDrainer(
            store=store, vector_store=mock_vs, max_retries=3,
        )
        processed = await drainer.process_once()

        assert processed == 1
        mock_vs.delete.assert_called_once_with("svc", ["id-1"])
        remaining = await store.load_pending()
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_increments_retry_on_failure(self) -> None:
        store = _FakeOutboxStore()
        event = VectorSyncEvent(collection="svc", pruned_ids=["id-1"])
        await store.write_event(event)

        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(side_effect=RuntimeError("Qdrant down"))

        drainer = DurableOutboxDrainer(
            store=store, vector_store=mock_vs, max_retries=3,
        )
        processed = await drainer.process_once()

        assert processed == 0
        remaining = await store.load_pending()
        assert len(remaining) == 1
        assert remaining[0].retry_count == 1

    @pytest.mark.asyncio
    async def test_drops_event_after_max_retries(self) -> None:
        store = _FakeOutboxStore()
        event = VectorSyncEvent(
            collection="svc", pruned_ids=["id-1"], retry_count=2,
        )
        await store.write_event(event)

        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(side_effect=RuntimeError("permanent"))

        drainer = DurableOutboxDrainer(
            store=store, vector_store=mock_vs, max_retries=3,
        )
        processed = await drainer.process_once()

        assert processed == 0
        remaining = await store.load_pending()
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_startup_recovery_loads_existing_events(self) -> None:
        store = _FakeOutboxStore()
        e1 = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        e2 = VectorSyncEvent(collection="svc", pruned_ids=["b"])
        await store.write_event(e1)
        await store.write_event(e2)

        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(return_value=1)

        drainer = DurableOutboxDrainer(
            store=store, vector_store=mock_vs, max_retries=5,
        )
        processed = await drainer.process_once()

        assert processed == 2
        remaining = await store.load_pending()
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_partial_failure_retains_failed_only(self) -> None:
        store = _FakeOutboxStore()
        events = []
        for i in range(3):
            evt = VectorSyncEvent(collection="svc", pruned_ids=[f"v{i}"])
            await store.write_event(evt)
            events.append(evt)

        call_idx = 0

        async def _fail_second(collection: str, ids: list) -> int:
            nonlocal call_idx
            call_idx += 1
            if call_idx == 2:
                raise RuntimeError("transient")
            return len(ids)

        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(side_effect=_fail_second)

        drainer = DurableOutboxDrainer(
            store=store, vector_store=mock_vs, max_retries=5,
        )
        processed = await drainer.process_once()

        assert processed == 2
        remaining = await store.load_pending()
        assert len(remaining) == 1
        assert remaining[0].retry_count == 1

    @pytest.mark.asyncio
    async def test_noop_when_store_empty(self) -> None:
        store = _FakeOutboxStore()
        mock_vs = AsyncMock()

        drainer = DurableOutboxDrainer(
            store=store, vector_store=mock_vs, max_retries=3,
        )
        processed = await drainer.process_once()

        assert processed == 0
        mock_vs.delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_idempotent_double_drain(self) -> None:
        store = _FakeOutboxStore()
        event = VectorSyncEvent(collection="svc", pruned_ids=["id-1"])
        await store.write_event(event)

        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(return_value=1)

        drainer = DurableOutboxDrainer(
            store=store, vector_store=mock_vs, max_retries=3,
        )
        first = await drainer.process_once()
        second = await drainer.process_once()

        assert first == 1
        assert second == 0

    @pytest.mark.asyncio
    async def test_version_fence_skips_stale_event(self) -> None:
        from orchestrator.app.vector_store import VectorRecord

        store = _FakeOutboxStore()
        newer = VectorSyncEvent(
            collection="svc",
            operation="upsert",
            pruned_ids=["id-1"],
            vectors=[VectorRecord(id="id-1", vector=[0.2], metadata={})],
            version=20,
        )
        older = VectorSyncEvent(
            collection="svc",
            operation="upsert",
            pruned_ids=["id-1"],
            vectors=[VectorRecord(id="id-1", vector=[0.1], metadata={})],
            version=10,
        )
        await store.write_event(newer)
        await store.write_event(older)

        mock_vs = AsyncMock()
        mock_vs.upsert = AsyncMock(return_value=1)
        mock_vs.delete = AsyncMock(return_value=1)

        drainer = DurableOutboxDrainer(
            store=store, vector_store=mock_vs, max_retries=3,
        )
        processed = await drainer.process_once()
        assert processed == 2
        mock_vs.upsert.assert_called_once()
        remaining = await store.load_pending()
        assert not remaining

    @pytest.mark.asyncio
    async def test_version_fence_persists_across_drainer_restart(self) -> None:
        from orchestrator.app.vector_store import VectorRecord

        store = _FakeOutboxStore()
        latest = VectorSyncEvent(
            collection="svc",
            operation="upsert",
            pruned_ids=["id-1"],
            vectors=[VectorRecord(id="id-1", vector=[0.3], metadata={})],
            version=30,
        )
        await store.write_event(latest)

        mock_vs = AsyncMock()
        mock_vs.upsert = AsyncMock(return_value=1)
        mock_vs.delete = AsyncMock(return_value=1)
        drainer_a = DurableOutboxDrainer(store=store, vector_store=mock_vs, max_retries=3)
        await drainer_a.process_once()

        stale = VectorSyncEvent(
            collection="svc",
            operation="upsert",
            pruned_ids=["id-1"],
            vectors=[VectorRecord(id="id-1", vector=[0.1], metadata={})],
            version=10,
        )
        await store.write_event(stale)
        drainer_b = DurableOutboxDrainer(store=store, vector_store=mock_vs, max_retries=3)
        await drainer_b.process_once()
        assert mock_vs.upsert.call_count == 1

    @pytest.mark.asyncio
    async def test_version_fence_isolated_per_tenant_scope(self) -> None:
        from orchestrator.app.vector_store import VectorRecord

        store = _FakeOutboxStore()
        t1_event = VectorSyncEvent(
            collection="service_embeddings",
            operation="upsert",
            pruned_ids=["id-1"],
            vectors=[VectorRecord(id="id-1", vector=[0.9], metadata={"tenant_id": "t1"})],
            version=20,
        )
        t2_event = VectorSyncEvent(
            collection="service_embeddings",
            operation="upsert",
            pruned_ids=["id-1"],
            vectors=[VectorRecord(id="id-1", vector=[0.1], metadata={"tenant_id": "t2"})],
            version=10,
        )
        await store.write_event(t1_event)
        await store.write_event(t2_event)

        mock_vs = AsyncMock()
        mock_vs.upsert = AsyncMock(return_value=1)
        mock_vs.delete = AsyncMock(return_value=1)
        drainer = DurableOutboxDrainer(store=store, vector_store=mock_vs, max_retries=3)
        await drainer.process_once()
        assert mock_vs.upsert.call_count == 2
