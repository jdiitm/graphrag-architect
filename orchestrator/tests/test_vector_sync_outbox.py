from __future__ import annotations

import asyncio
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.vector_sync_outbox import (
    ClaimableOutboxStore,
    DurableOutboxDrainer,
    OutboxDrainer,
    OutboxStore,
    RedisOutboxStore,
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


class TestVectorSyncEventUpsertOperation:

    def test_event_supports_upsert_operation(self) -> None:
        from orchestrator.app.vector_store import VectorRecord
        event = VectorSyncEvent(
            collection="services",
            operation="upsert",
            vectors=[
                VectorRecord(id="v1", vector=[0.1, 0.2], metadata={"name": "svc"}),
            ],
        )
        assert event.operation == "upsert"
        assert len(event.vectors) == 1

    def test_delete_operation_is_default(self) -> None:
        event = VectorSyncEvent(
            collection="services", pruned_ids=["id-1"],
        )
        assert event.operation == "delete"

    def test_upsert_event_does_not_require_pruned_ids(self) -> None:
        from orchestrator.app.vector_store import VectorRecord
        event = VectorSyncEvent(
            collection="services",
            operation="upsert",
            vectors=[
                VectorRecord(id="v1", vector=[0.1], metadata={}),
            ],
        )
        assert event.pruned_ids == []


class TestOutboxDrainerUpsertHandling:

    @pytest.mark.asyncio
    async def test_drainer_processes_upsert_events(self) -> None:
        from orchestrator.app.vector_store import VectorRecord
        outbox = VectorSyncOutbox()
        mock_vs = AsyncMock()
        mock_vs.upsert = AsyncMock(return_value=1)

        event = VectorSyncEvent(
            collection="services",
            operation="upsert",
            vectors=[
                VectorRecord(id="v1", vector=[0.1, 0.2], metadata={"name": "svc"}),
            ],
        )
        outbox.enqueue(event)

        drainer = OutboxDrainer(outbox=outbox, vector_store=mock_vs)
        processed = await drainer.process_once()

        assert processed == 1
        mock_vs.upsert.assert_called_once_with("services", event.vectors)
        assert outbox.pending_count == 0

    @pytest.mark.asyncio
    async def test_drainer_handles_mixed_operations(self) -> None:
        from orchestrator.app.vector_store import VectorRecord
        outbox = VectorSyncOutbox()
        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(return_value=1)
        mock_vs.upsert = AsyncMock(return_value=1)

        delete_event = VectorSyncEvent(
            collection="svc", pruned_ids=["old-1"],
        )
        upsert_event = VectorSyncEvent(
            collection="svc",
            operation="upsert",
            vectors=[
                VectorRecord(id="new-1", vector=[0.5], metadata={}),
            ],
        )
        outbox.enqueue(delete_event)
        outbox.enqueue(upsert_event)

        drainer = OutboxDrainer(outbox=outbox, vector_store=mock_vs)
        processed = await drainer.process_once()

        assert processed == 2
        mock_vs.delete.assert_called_once()
        mock_vs.upsert.assert_called_once()


class TestRedisOutboxStore:

    @pytest.mark.asyncio
    async def test_write_event_persists_to_redis(self) -> None:
        mock_redis = AsyncMock()
        store = RedisOutboxStore(redis_conn=mock_redis)
        event = VectorSyncEvent(collection="svc", pruned_ids=["a", "b"])
        await store.write_event(event)
        mock_redis.hset.assert_called_once()
        call_args = mock_redis.hset.call_args
        assert event.event_id in str(call_args)

    @pytest.mark.asyncio
    async def test_load_pending_returns_stored_events(self) -> None:
        mock_redis = AsyncMock()
        event = VectorSyncEvent(collection="svc", pruned_ids=["x"])
        mock_redis.smembers = AsyncMock(return_value={event.event_id})
        mock_redis.hgetall = AsyncMock(return_value={
            "event_id": event.event_id,
            "collection": "svc",
            "pruned_ids": '["x"]',
            "status": "pending",
            "retry_count": "0",
        })
        store = RedisOutboxStore(redis_conn=mock_redis)
        pending = await store.load_pending()
        assert len(pending) == 1
        assert pending[0].collection == "svc"
        assert pending[0].pruned_ids == ["x"]

    @pytest.mark.asyncio
    async def test_delete_event_removes_from_redis(self) -> None:
        mock_redis = AsyncMock()
        store = RedisOutboxStore(redis_conn=mock_redis)
        await store.delete_event("evt-123")
        mock_redis.delete.assert_called()
        mock_redis.srem.assert_called()

    @pytest.mark.asyncio
    async def test_update_retry_count_increments_field(self) -> None:
        mock_redis = AsyncMock()
        store = RedisOutboxStore(redis_conn=mock_redis)
        await store.update_retry_count("evt-123", 3)
        mock_redis.hset.assert_called_once()

    @pytest.mark.asyncio
    async def test_load_pending_skips_missing_keys(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.smembers = AsyncMock(return_value={"gone-event"})
        mock_redis.hgetall = AsyncMock(return_value={})
        store = RedisOutboxStore(redis_conn=mock_redis)
        pending = await store.load_pending()
        assert len(pending) == 0


class FakeRedis:

    def __init__(self) -> None:
        self._hashes: Dict[str, Dict[str, str]] = {}
        self._sets: Dict[str, set] = {}
        self._strings: Dict[str, str] = {}

    async def hset(
        self,
        name: str,
        key: str | None = None,
        value: str | None = None,
        mapping: Dict[str, str] | None = None,
    ) -> None:
        if name not in self._hashes:
            self._hashes[name] = {}
        if mapping:
            self._hashes[name].update(mapping)
        if key is not None and value is not None:
            self._hashes[name][key] = value

    async def hgetall(self, name: str) -> Dict[str, str]:
        return dict(self._hashes.get(name, {}))

    async def sadd(self, name: str, *values: str) -> None:
        if name not in self._sets:
            self._sets[name] = set()
        self._sets[name].update(values)

    async def smembers(self, name: str) -> set:
        return set(self._sets.get(name, set()))

    async def srem(self, name: str, *values: str) -> None:
        if name in self._sets:
            self._sets[name] -= set(values)

    async def delete(self, name: str) -> None:
        self._hashes.pop(name, None)

    async def get(self, name: str) -> str | None:
        return self._strings.get(name)

    async def set(self, name: str, value: str) -> None:
        self._strings[name] = value

    async def eval(
        self,
        script: str,
        numkeys: int,
        *args: Any,
    ) -> list:
        index_key = args[0]
        worker_id = args[1]
        limit = int(args[2])
        lease_seconds = float(args[3])
        now = float(args[4])
        prefix = args[5]
        event_ids = list(self._sets.get(index_key, set()))
        claimed: List[str] = []
        for eid in event_ids:
            if len(claimed) >= limit:
                break
            key = prefix + eid
            data = self._hashes.get(key)
            if data is None:
                if index_key in self._sets:
                    self._sets[index_key].discard(eid)
                continue
            status = data.get("status", "pending")
            if status == "pending":
                data["status"] = "claimed"
                data["claimed_by"] = worker_id
                data["lease_expires_at"] = str(now + lease_seconds)
                claimed.append(eid)
            elif status == "claimed":
                expires = float(data.get("lease_expires_at", "0"))
                if expires <= now:
                    data["status"] = "claimed"
                    data["claimed_by"] = worker_id
                    data["lease_expires_at"] = str(now + lease_seconds)
                    claimed.append(eid)
        return claimed


class TestRedisOutboxStoreUpsertRoundTrip:

    @pytest.mark.asyncio
    async def test_upsert_event_survives_write_load_roundtrip(self) -> None:
        from orchestrator.app.vector_store import VectorRecord
        fake_redis = FakeRedis()
        store = RedisOutboxStore(redis_conn=fake_redis)

        original = VectorSyncEvent(
            collection="services",
            operation="upsert",
            vectors=[
                VectorRecord(
                    id="v1", vector=[0.1, 0.2], metadata={"name": "auth"},
                ),
                VectorRecord(
                    id="v2", vector=[0.3, 0.4], metadata={"name": "payments"},
                ),
            ],
        )
        await store.write_event(original)
        loaded = await store.load_pending()

        assert len(loaded) == 1
        event = loaded[0]
        assert event.event_id == original.event_id
        assert event.operation == "upsert"
        assert len(event.vectors) == 2

        for vec in event.vectors:
            assert isinstance(vec, VectorRecord), (
                f"Expected VectorRecord, got {type(vec).__name__}"
            )

        assert event.vectors[0].id == "v1"
        assert event.vectors[0].vector == [0.1, 0.2]
        assert event.vectors[0].metadata == {"name": "auth"}
        assert event.vectors[1].id == "v2"
        assert event.vectors[1].vector == [0.3, 0.4]
        assert event.vectors[1].metadata == {"name": "payments"}

    @pytest.mark.asyncio
    async def test_delete_event_roundtrip_preserves_operation(self) -> None:
        fake_redis = FakeRedis()
        store = RedisOutboxStore(redis_conn=fake_redis)

        original = VectorSyncEvent(
            collection="services",
            operation="delete",
            pruned_ids=["old-1", "old-2"],
        )
        await store.write_event(original)
        loaded = await store.load_pending()

        assert len(loaded) == 1
        event = loaded[0]
        assert event.operation == "delete"
        assert event.pruned_ids == ["old-1", "old-2"]
        assert event.vectors == []


class SpyVectorStore:

    def __init__(self) -> None:
        self.upsert_calls: List[Any] = []
        self.delete_calls: List[Any] = []

    async def upsert(self, collection: str, vectors: List[Any]) -> int:
        from orchestrator.app.vector_store import VectorRecord
        for record in vectors:
            _ = record.id
            _ = record.vector
            _ = record.metadata
            assert isinstance(record, VectorRecord), (
                f"Expected VectorRecord, got {type(record).__name__}"
            )
        self.upsert_calls.append((collection, vectors))
        return len(vectors)

    async def delete(self, collection: str, ids: List[str]) -> int:
        self.delete_calls.append((collection, ids))
        return len(ids)


class TestDurableOutboxDrainerUpsertIntegration:

    @pytest.mark.asyncio
    async def test_durable_drainer_processes_upsert_via_store_roundtrip(
        self,
    ) -> None:
        from orchestrator.app.vector_store import VectorRecord
        fake_redis = FakeRedis()
        store = RedisOutboxStore(redis_conn=fake_redis)
        spy_vs = SpyVectorStore()

        event = VectorSyncEvent(
            collection="services",
            operation="upsert",
            vectors=[
                VectorRecord(
                    id="v1", vector=[0.1, 0.2], metadata={"name": "svc"},
                ),
            ],
        )
        await store.write_event(event)

        drainer = DurableOutboxDrainer(
            store=store, vector_store=spy_vs, max_retries=3,
        )
        processed = await drainer.process_once()

        assert processed == 1
        assert len(spy_vs.upsert_calls) == 1
        call_collection, call_vectors = spy_vs.upsert_calls[0]
        assert call_collection == "services"
        assert len(call_vectors) == 1
        assert isinstance(call_vectors[0], VectorRecord)
        assert call_vectors[0].id == "v1"
        assert call_vectors[0].vector == [0.1, 0.2]
        assert call_vectors[0].metadata == {"name": "svc"}


class TestDurableOutboxDrainerIntegration:

    @pytest.mark.asyncio
    async def test_durable_drainer_processes_and_deletes_events(self) -> None:
        mock_store = AsyncMock(spec=OutboxStore)
        event = VectorSyncEvent(collection="svc", pruned_ids=["v1"])
        mock_store.load_pending = AsyncMock(return_value=[event])
        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(return_value=1)

        drainer = DurableOutboxDrainer(
            store=mock_store, vector_store=mock_vs, max_retries=3,
        )
        processed = await drainer.process_once()

        assert processed == 1
        mock_store.delete_event.assert_called_once_with(event.event_id)
        mock_vs.delete.assert_called_once_with("svc", ["v1"])

    @pytest.mark.asyncio
    async def test_durable_drainer_retries_on_failure(self) -> None:
        mock_store = AsyncMock(spec=OutboxStore)
        event = VectorSyncEvent(collection="svc", pruned_ids=["v1"])
        event.retry_count = 0
        mock_store.load_pending = AsyncMock(return_value=[event])
        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(side_effect=RuntimeError("qdrant down"))

        drainer = DurableOutboxDrainer(
            store=mock_store, vector_store=mock_vs, max_retries=3,
        )
        processed = await drainer.process_once()

        assert processed == 0
        mock_store.update_retry_count.assert_called_once_with(
            event.event_id, 1,
        )

    @pytest.mark.asyncio
    async def test_durable_drainer_discards_after_max_retries(self) -> None:
        mock_store = AsyncMock(spec=OutboxStore)
        event = VectorSyncEvent(collection="svc", pruned_ids=["v1"])
        event.retry_count = 4
        mock_store.load_pending = AsyncMock(return_value=[event])
        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(side_effect=RuntimeError("qdrant down"))

        drainer = DurableOutboxDrainer(
            store=mock_store, vector_store=mock_vs, max_retries=5,
        )
        processed = await drainer.process_once()

        assert processed == 0
        mock_store.delete_event.assert_called_once_with(event.event_id)


class ClaimableFakeRedis(FakeRedis):

    async def hset(
        self,
        name: str,
        key: str | None = None,
        value: str | None = None,
        mapping: Dict[str, str] | None = None,
    ) -> None:
        if name not in self._hashes:
            self._hashes[name] = {}
        if mapping:
            self._hashes[name].update(mapping)
        if key is not None and value is not None:
            self._hashes[name][key] = str(value)

    async def eval(
        self,
        script: str,
        numkeys: int,
        *args: Any,
    ) -> list:
        import time as _time
        index_key = args[0]
        worker_id = args[1]
        limit = int(args[2])
        lease_seconds = float(args[3])
        now = float(args[4])
        prefix = args[5]
        event_ids = list(self._sets.get(index_key, set()))
        claimed: List[str] = []
        for eid in event_ids:
            if len(claimed) >= limit:
                break
            key = prefix + eid
            data = self._hashes.get(key)
            if data is None:
                if index_key in self._sets:
                    self._sets[index_key].discard(eid)
                continue
            status = data.get("status", "pending")
            if status == "pending":
                data["status"] = "claimed"
                data["claimed_by"] = worker_id
                data["lease_expires_at"] = str(now + lease_seconds)
                claimed.append(eid)
            elif status == "claimed":
                expires = float(data.get("lease_expires_at", "0"))
                if expires <= now:
                    data["status"] = "claimed"
                    data["claimed_by"] = worker_id
                    data["lease_expires_at"] = str(now + lease_seconds)
                    claimed.append(eid)
        return claimed


class TestRedisOutboxStoreClaimPending:

    @pytest.mark.asyncio
    async def test_claim_returns_only_pending_events(self) -> None:
        redis = ClaimableFakeRedis()
        store = RedisOutboxStore(redis_conn=redis)
        e1 = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        e2 = VectorSyncEvent(collection="svc", pruned_ids=["b"])
        await store.write_event(e1)
        await store.write_event(e2)

        claimed = await store.claim_pending("worker-1", limit=10, lease_seconds=60.0)
        assert len(claimed) == 2

    @pytest.mark.asyncio
    async def test_claim_prevents_double_claiming(self) -> None:
        redis = ClaimableFakeRedis()
        store = RedisOutboxStore(redis_conn=redis)
        event = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        await store.write_event(event)

        first = await store.claim_pending("worker-1", limit=10, lease_seconds=60.0)
        assert len(first) == 1

        second = await store.claim_pending("worker-2", limit=10, lease_seconds=60.0)
        assert len(second) == 0

    @pytest.mark.asyncio
    async def test_mark_completed_prevents_reprocessing(self) -> None:
        redis = ClaimableFakeRedis()
        store = RedisOutboxStore(redis_conn=redis)
        event = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        await store.write_event(event)

        claimed = await store.claim_pending("worker-1", limit=10, lease_seconds=60.0)
        assert len(claimed) == 1
        await store.mark_completed(event.event_id)

        reclaimed = await store.claim_pending("worker-2", limit=10, lease_seconds=60.0)
        assert len(reclaimed) == 0


class TestRedisOutboxStoreReleaseExpiredClaims:

    @pytest.mark.asyncio
    async def test_expired_claim_becomes_reclaimable(self) -> None:
        redis = ClaimableFakeRedis()
        store = RedisOutboxStore(redis_conn=redis)
        event = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        await store.write_event(event)

        await store.claim_pending("worker-1", limit=10, lease_seconds=0.0)

        released = await store.release_expired_claims()
        assert released >= 1

        reclaimed = await store.claim_pending("worker-2", limit=10, lease_seconds=60.0)
        assert len(reclaimed) == 1

    @pytest.mark.asyncio
    async def test_active_claim_not_released(self) -> None:
        redis = ClaimableFakeRedis()
        store = RedisOutboxStore(redis_conn=redis)
        event = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        await store.write_event(event)

        await store.claim_pending("worker-1", limit=10, lease_seconds=300.0)
        released = await store.release_expired_claims()
        assert released == 0


class TestDurableOutboxDrainerWithClaiming:

    @pytest.mark.asyncio
    async def test_drainer_uses_claiming_when_available(self) -> None:
        redis = ClaimableFakeRedis()
        store = RedisOutboxStore(redis_conn=redis)
        spy_vs = SpyVectorStore()

        event = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        await store.write_event(event)

        drainer = DurableOutboxDrainer(
            store=store, vector_store=spy_vs, max_retries=3,
            worker_id="test-worker",
        )
        processed = await drainer.process_once()

        assert processed == 1
        assert len(spy_vs.delete_calls) == 1
        remaining = await store.load_pending()
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_drainer_prevents_parallel_processing(self) -> None:
        # NOTE: This test runs drainers sequentially (await d1; await d2),
        # which validates claim semantics but does NOT exercise true
        # concurrent interleaving.  RedisOutboxStore.claim_pending uses
        # non-atomic read-check-set; concurrent coroutines can double-claim.
        # Production multi-worker deployments should use a Redis Lua script
        # for atomic claiming — see RedisOutboxStore class docstring.
        redis = ClaimableFakeRedis()
        store = RedisOutboxStore(redis_conn=redis)
        spy_vs_1 = SpyVectorStore()
        spy_vs_2 = SpyVectorStore()

        event = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        await store.write_event(event)

        drainer_1 = DurableOutboxDrainer(
            store=store, vector_store=spy_vs_1, max_retries=3,
            worker_id="worker-1",
        )
        drainer_2 = DurableOutboxDrainer(
            store=store, vector_store=spy_vs_2, max_retries=3,
            worker_id="worker-2",
        )

        p1 = await drainer_1.process_once()
        p2 = await drainer_2.process_once()

        total_deletes = len(spy_vs_1.delete_calls) + len(spy_vs_2.delete_calls)
        assert total_deletes == 1
        assert p1 + p2 == 1

    @pytest.mark.asyncio
    async def test_failed_claimed_event_resets_to_pending(self) -> None:
        redis = ClaimableFakeRedis()
        store = RedisOutboxStore(redis_conn=redis)

        class FailingVectorStore:
            async def delete(self, collection: str, ids: list) -> int:
                raise RuntimeError("transient failure")

        event = VectorSyncEvent(collection="svc", pruned_ids=["a"])
        await store.write_event(event)

        drainer = DurableOutboxDrainer(
            store=store, vector_store=FailingVectorStore(), max_retries=5,
            worker_id="worker-1", lease_seconds=300.0,
        )
        await drainer.process_once()

        reclaimed = await store.claim_pending(
            "worker-2", limit=10, lease_seconds=300.0,
        )
        assert len(reclaimed) == 1, (
            "Failed event should be immediately reclaimable, "
            "not stuck behind a 300s lease"
        )
        assert reclaimed[0].retry_count == 1

    @pytest.mark.asyncio
    async def test_drainer_backward_compat_without_claiming(self) -> None:
        mock_store = AsyncMock(spec=OutboxStore)
        event = VectorSyncEvent(collection="svc", pruned_ids=["v1"])
        mock_store.load_pending = AsyncMock(return_value=[event])
        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(return_value=1)

        drainer = DurableOutboxDrainer(
            store=mock_store, vector_store=mock_vs, max_retries=3,
        )
        processed = await drainer.process_once()

        assert processed == 1
        mock_store.load_pending.assert_called_once()
        mock_store.delete_event.assert_called_once_with(event.event_id)
