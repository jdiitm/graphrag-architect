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

    async def test_max_tracked_fences_defaults_from_env(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("OUTBOX_MAX_TRACKED_FENCES", "42")
        drainer = DurableOutboxDrainer(
            store=_FakeOutboxStore(),
            vector_store=AsyncMock(),
        )
        assert drainer._max_tracked_fences == 42


@pytest.mark.asyncio
class TestFenceCacheEvictionAtCapacity:

    async def test_evicts_oldest_when_over_capacity(self) -> None:
        drainer = _make_drainer(max_tracked_fences=3)

        drainer._fence_record("key-a", 100)
        drainer._fence_record("key-b", 200)
        drainer._fence_record("key-c", 300)
        assert len(drainer._latest_version_by_key) == 3

        drainer._fence_record("key-d", 400)
        assert len(drainer._latest_version_by_key) == 3
        assert "key-a" not in drainer._latest_version_by_key
        assert drainer._fence_lookup("key-d") == 400

    async def test_evicts_lru_not_most_recently_written(self) -> None:
        drainer = _make_drainer(max_tracked_fences=3)

        drainer._fence_record("key-a", 100)
        drainer._fence_record("key-b", 200)
        drainer._fence_record("key-c", 300)

        drainer._fence_lookup("key-a")

        drainer._fence_record("key-d", 400)
        assert "key-a" in drainer._latest_version_by_key
        assert "key-b" not in drainer._latest_version_by_key

    async def test_no_eviction_under_capacity(self) -> None:
        drainer = _make_drainer(max_tracked_fences=10)

        for i in range(10):
            drainer._fence_record(f"key-{i}", i * 100)

        assert len(drainer._latest_version_by_key) == 10


@pytest.mark.asyncio
class TestFenceLookupRefreshesLRU:

    async def test_lookup_moves_key_to_end(self) -> None:
        drainer = _make_drainer(max_tracked_fences=100)

        drainer._fence_record("old", 1)
        drainer._fence_record("new", 2)

        drainer._fence_lookup("old")

        keys = list(drainer._latest_version_by_key.keys())
        assert keys[-1] == "old"

    async def test_lookup_miss_does_not_move(self) -> None:
        drainer = _make_drainer(max_tracked_fences=100)
        drainer._fence_record("only-key", 1)

        result = drainer._fence_lookup("missing")
        assert result == -1
        assert list(drainer._latest_version_by_key.keys()) == ["only-key"]


@pytest.mark.asyncio
class TestFenceEvictedKeyReencounter:

    async def test_evicted_key_processed_again_safely(self) -> None:
        store = _FakeOutboxStore()
        drainer = _make_drainer(store=store, max_tracked_fences=2)

        e1 = VectorSyncEvent(
            collection="svc", pruned_ids=["a"], version=100,
        )
        await store.write_event(e1)
        await drainer.process_once()

        drainer._fence_record("filler-1", 200)
        drainer._fence_record("filler-2", 300)
        fence_key = list(drainer._latest_version_by_key.keys())
        assert all("svc" not in k or k.startswith("filler") for k in fence_key) or \
            len(drainer._latest_version_by_key) == 2

        e2 = VectorSyncEvent(
            collection="svc", pruned_ids=["a"], version=400,
        )
        await store.write_event(e2)
        processed = await drainer.process_once()

        assert processed == 1

    async def test_full_integration_with_bounded_cache(self) -> None:
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
        assert len(drainer._latest_version_by_key) == 3
        assert mock_vs.delete.call_count == 5
