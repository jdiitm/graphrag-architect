from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from orchestrator.app.vector_sync_outbox import (
    CoalescingOutbox,
    RedisOutboxStore,
    VectorSyncEvent,
)


class TestCoalescingDeduplicatesSameNode:

    def test_hundred_rapid_events_coalesce_to_one(self) -> None:
        outbox = CoalescingOutbox(window_seconds=0.0)
        for _ in range(100):
            outbox.enqueue(VectorSyncEvent(
                collection="services",
                pruned_ids=["node-a"],
            ))
        flushed = outbox.flush()
        assert len(flushed) == 1
        assert flushed[0].collection == "services"
        assert flushed[0].pruned_ids == ["node-a"]

    def test_pending_count_reflects_coalesced_state(self) -> None:
        outbox = CoalescingOutbox(window_seconds=0.0)
        for _ in range(50):
            outbox.enqueue(VectorSyncEvent(
                collection="services",
                pruned_ids=["node-a"],
            ))
        assert outbox.pending_count == 1

    def test_latest_event_id_survives(self) -> None:
        outbox = CoalescingOutbox(window_seconds=0.0)
        last_event = None
        for _ in range(100):
            last_event = VectorSyncEvent(
                collection="services", pruned_ids=["node-a"],
            )
            outbox.enqueue(last_event)
        flushed = outbox.flush()
        assert flushed[0].event_id == last_event.event_id

    def test_flush_clears_buffer(self) -> None:
        outbox = CoalescingOutbox(window_seconds=0.0)
        outbox.enqueue(VectorSyncEvent(
            collection="services", pruned_ids=["node-a"],
        ))
        outbox.flush()
        assert outbox.pending_count == 0
        assert outbox.flush() == []


class TestDistinctNodesNotCoalesced:

    def test_distinct_node_ids_produce_distinct_events(self) -> None:
        outbox = CoalescingOutbox(window_seconds=0.0)
        for i in range(10):
            outbox.enqueue(VectorSyncEvent(
                collection="services",
                pruned_ids=[f"node-{i}"],
            ))
        flushed = outbox.flush()
        assert len(flushed) == 10

    def test_same_ids_different_collections_not_coalesced(self) -> None:
        outbox = CoalescingOutbox(window_seconds=0.0)
        outbox.enqueue(VectorSyncEvent(
            collection="services", pruned_ids=["node-a"],
        ))
        outbox.enqueue(VectorSyncEvent(
            collection="deployments", pruned_ids=["node-a"],
        ))
        flushed = outbox.flush()
        assert len(flushed) == 2


class TestFlushWindowHoldsEvents:

    def test_drain_pending_empty_before_window(self) -> None:
        outbox = CoalescingOutbox(window_seconds=10.0)
        outbox.enqueue(VectorSyncEvent(
            collection="services", pruned_ids=["node-a"],
        ))
        ready = outbox.drain_pending()
        assert len(ready) == 0
        assert outbox.pending_count == 1

    def test_drain_pending_returns_events_after_window(self) -> None:
        outbox = CoalescingOutbox(window_seconds=0.0)
        outbox.enqueue(VectorSyncEvent(
            collection="services", pruned_ids=["node-a"],
        ))
        ready = outbox.drain_pending()
        assert len(ready) == 1

    def test_drain_pending_with_mocked_time(self) -> None:
        outbox = CoalescingOutbox(window_seconds=5.0)
        outbox.enqueue(VectorSyncEvent(
            collection="services", pruned_ids=["node-a"],
        ))
        assert len(outbox.drain_pending()) == 0

        with patch(
            "orchestrator.app.vector_sync_outbox.time.monotonic",
            return_value=time.monotonic() + 10.0,
        ):
            ready = outbox.drain_pending()
        assert len(ready) == 1
        assert outbox.pending_count == 0

    def test_drain_removes_only_expired_entries(self) -> None:
        outbox = CoalescingOutbox(window_seconds=5.0)
        outbox.enqueue(VectorSyncEvent(
            collection="services", pruned_ids=["old-node"],
        ))
        future = time.monotonic() + 10.0
        with patch(
            "orchestrator.app.vector_sync_outbox.time.monotonic",
            return_value=future,
        ):
            outbox.enqueue(VectorSyncEvent(
                collection="services", pruned_ids=["new-node"],
            ))
            ready = outbox.drain_pending()
        assert len(ready) == 1
        assert ready[0].pruned_ids == ["old-node"]
        assert outbox.pending_count == 1


class TestMixedOperationsCoalesce:

    def test_delete_overwrites_prior_upsert_for_same_node(self) -> None:
        from orchestrator.app.vector_store import VectorRecord
        outbox = CoalescingOutbox(window_seconds=0.0)
        outbox.enqueue(VectorSyncEvent(
            collection="services",
            operation="upsert",
            vectors=[VectorRecord(id="node-a", vector=[0.1], metadata={})],
        ))
        outbox.enqueue(VectorSyncEvent(
            collection="services",
            operation="delete",
            pruned_ids=["node-a"],
        ))
        flushed = outbox.flush()
        assert len(flushed) == 1
        assert flushed[0].operation == "delete"

    def test_upsert_overwrites_prior_delete_for_same_node(self) -> None:
        from orchestrator.app.vector_store import VectorRecord
        outbox = CoalescingOutbox(window_seconds=0.0)
        outbox.enqueue(VectorSyncEvent(
            collection="services",
            operation="delete",
            pruned_ids=["node-a"],
        ))
        outbox.enqueue(VectorSyncEvent(
            collection="services",
            operation="upsert",
            vectors=[VectorRecord(id="node-a", vector=[0.1], metadata={})],
        ))
        flushed = outbox.flush()
        assert len(flushed) == 1
        assert flushed[0].operation == "upsert"


class TestRedisDeduplication:

    @pytest.mark.asyncio
    async def test_write_dedup_replaces_same_node_event(self) -> None:
        from orchestrator.tests.test_vector_sync_outbox import FakeRedis

        fake_redis = FakeRedis()
        store = RedisOutboxStore(redis_conn=fake_redis)

        event_1 = VectorSyncEvent(
            collection="services", pruned_ids=["node-a"],
        )
        event_2 = VectorSyncEvent(
            collection="services", pruned_ids=["node-a"],
        )
        await store.write_dedup_event(event_1)
        await store.write_dedup_event(event_2)

        pending = await store.load_pending()
        assert len(pending) == 1
        assert pending[0].event_id == event_2.event_id

    @pytest.mark.asyncio
    async def test_dedup_preserves_distinct_nodes(self) -> None:
        from orchestrator.tests.test_vector_sync_outbox import FakeRedis

        fake_redis = FakeRedis()
        store = RedisOutboxStore(redis_conn=fake_redis)

        event_a = VectorSyncEvent(
            collection="services", pruned_ids=["node-a"],
        )
        event_b = VectorSyncEvent(
            collection="services", pruned_ids=["node-b"],
        )
        await store.write_dedup_event(event_a)
        await store.write_dedup_event(event_b)

        pending = await store.load_pending()
        assert len(pending) == 2
