from __future__ import annotations

from collections import OrderedDict
from typing import List
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.vector_sync_outbox import (
    DurableOutboxDrainer,
    VectorSyncEvent,
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


def _make_drainer(
    store: _FakeOutboxStore | None = None,
    max_tracked_fences: int = 5,
) -> DurableOutboxDrainer:
    drainer = DurableOutboxDrainer(
        store=store or _FakeOutboxStore(),
        vector_store=AsyncMock(delete=AsyncMock(return_value=1)),
        max_retries=5,
    )
    drainer._max_tracked_fences = max_tracked_fences
    return drainer


@pytest.mark.asyncio
class TestFenceCacheUsesOrderedDict:

    async def test_latest_version_by_key_is_ordered_dict(self) -> None:
        drainer = _make_drainer()
        assert isinstance(drainer._latest_version_by_key, OrderedDict)
        assert len(drainer._latest_version_by_key) == 0

    async def test_max_tracked_fences_defaults_from_env(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("OUTBOX_MAX_TRACKED_FENCES", "42")
        drainer = DurableOutboxDrainer(
            store=_FakeOutboxStore(),
            vector_store=AsyncMock(),
        )
        assert drainer._max_tracked_fences == 42

    async def test_max_tracked_fences_uses_default_without_env(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("OUTBOX_MAX_TRACKED_FENCES", raising=False)
        drainer = DurableOutboxDrainer(
            store=_FakeOutboxStore(),
            vector_store=AsyncMock(),
        )
        assert drainer._max_tracked_fences == 100_000


@pytest.mark.asyncio
class TestFenceCacheEvictionAtCapacity:

    async def test_evicts_oldest_when_over_capacity(self) -> None:
        drainer = _make_drainer(max_tracked_fences=3)

        drainer._fence_record("key-a", 100)
        drainer._fence_record("key-b", 200)
        drainer._fence_record("key-c", 300)
        assert len(drainer._latest_version_by_key) == 3
        assert set(drainer._latest_version_by_key.keys()) == {
            "key-a", "key-b", "key-c",
        }

        drainer._fence_record("key-d", 400)
        assert len(drainer._latest_version_by_key) == 3
        assert set(drainer._latest_version_by_key.keys()) == {
            "key-b", "key-c", "key-d",
        }
        assert drainer._fence_lookup("key-a") == -1
        assert drainer._fence_lookup("key-d") == 400

    async def test_evicts_lru_not_most_recently_written(self) -> None:
        drainer = _make_drainer(max_tracked_fences=3)

        drainer._fence_record("key-a", 100)
        drainer._fence_record("key-b", 200)
        drainer._fence_record("key-c", 300)

        drainer._fence_lookup("key-a")

        drainer._fence_record("key-d", 400)
        assert set(drainer._latest_version_by_key.keys()) == {
            "key-a", "key-c", "key-d",
        }
        assert drainer._fence_lookup("key-b") == -1

    async def test_no_eviction_at_exactly_capacity(self) -> None:
        drainer = _make_drainer(max_tracked_fences=5)

        for i in range(5):
            drainer._fence_record(f"key-{i}", i * 100)

        assert len(drainer._latest_version_by_key) == 5
        for i in range(5):
            assert drainer._fence_lookup(f"key-{i}") == i * 100

    async def test_repeated_writes_to_same_key_do_not_grow_cache(self) -> None:
        drainer = _make_drainer(max_tracked_fences=3)

        drainer._fence_record("key-a", 100)
        drainer._fence_record("key-a", 200)
        drainer._fence_record("key-a", 300)
        drainer._fence_record("key-a", 400)

        assert len(drainer._latest_version_by_key) == 1
        assert drainer._fence_lookup("key-a") == 400


@pytest.mark.asyncio
class TestFenceLookupRefreshesLRU:

    async def test_lookup_moves_key_to_end(self) -> None:
        drainer = _make_drainer(max_tracked_fences=100)

        drainer._fence_record("first", 1)
        drainer._fence_record("second", 2)
        drainer._fence_record("third", 3)

        drainer._fence_lookup("first")

        keys = list(drainer._latest_version_by_key.keys())
        assert keys == ["second", "third", "first"]

    async def test_lookup_miss_returns_negative_one_without_side_effects(self) -> None:
        drainer = _make_drainer(max_tracked_fences=100)
        drainer._fence_record("only-key", 1)

        result = drainer._fence_lookup("missing")

        assert result == -1
        assert len(drainer._latest_version_by_key) == 1
        assert list(drainer._latest_version_by_key.keys()) == ["only-key"]

    async def test_lookup_returns_recorded_version(self) -> None:
        drainer = _make_drainer(max_tracked_fences=100)
        drainer._fence_record("k", 42)

        assert drainer._fence_lookup("k") == 42


@pytest.mark.asyncio
class TestFenceEvictedKeyReencounter:

    async def test_stale_event_replayed_after_fence_eviction(self) -> None:
        store = _FakeOutboxStore()
        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(return_value=1)
        drainer = DurableOutboxDrainer(
            store=store, vector_store=mock_vs, max_retries=5,
        )
        drainer._max_tracked_fences = 2

        e1 = VectorSyncEvent(
            collection="svc", pruned_ids=["a"], version=100,
        )
        await store.write_event(e1)
        await drainer.process_once()
        assert mock_vs.delete.call_count == 1

        original_fence_key = list(drainer._latest_version_by_key.keys())[0]
        drainer._fence_record("evict-filler-1", 200)
        drainer._fence_record("evict-filler-2", 300)
        assert original_fence_key not in drainer._latest_version_by_key

        e2 = VectorSyncEvent(
            collection="svc", pruned_ids=["a"], version=50,
        )
        await store.write_event(e2)
        processed = await drainer.process_once()

        assert processed == 1
        assert mock_vs.delete.call_count == 2

    async def test_full_integration_bounded_cache_evicts_oldest_fences(self) -> None:
        store = _FakeOutboxStore()
        mock_vs = AsyncMock()
        mock_vs.delete = AsyncMock(return_value=1)

        drainer = DurableOutboxDrainer(
            store=store, vector_store=mock_vs, max_retries=5,
        )
        drainer._max_tracked_fences = 3

        for i in range(5):
            event = VectorSyncEvent(
                collection=f"coll-{i}", pruned_ids=[f"id-{i}"],
                version=i * 100,
            )
            await store.write_event(event)

        processed = await drainer.process_once()

        assert processed == 5
        assert mock_vs.delete.call_count == 5
        assert len(drainer._latest_version_by_key) == 3

        remaining_versions = list(drainer._latest_version_by_key.values())
        assert remaining_versions == [200, 300, 400]
