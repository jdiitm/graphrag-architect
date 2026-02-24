from __future__ import annotations

import asyncio
import os
import tempfile

import pytest

from orchestrator.app.completion_tracker import (
    CompletionTracker,
    FileCompletionStore,
)


@pytest.fixture()
def tracker_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture()
def tracker(tracker_dir: str) -> CompletionTracker:
    store = FileCompletionStore(tracker_dir)
    return CompletionTracker(store)


class TestFileCompletionStore:
    def test_mark_complete_creates_record(
        self, tracker_dir: str,
    ) -> None:
        store = FileCompletionStore(tracker_dir)
        asyncio.run(store.mark("abc123"))
        entries = os.listdir(tracker_dir)
        assert len(entries) == 1

    def test_is_complete_returns_true_after_mark(
        self, tracker_dir: str,
    ) -> None:
        store = FileCompletionStore(tracker_dir)

        async def _run():
            await store.mark("hash-001")
            return await store.exists("hash-001")

        assert asyncio.run(_run()) is True

    def test_is_complete_returns_false_for_unknown(
        self, tracker_dir: str,
    ) -> None:
        store = FileCompletionStore(tracker_dir)
        assert asyncio.run(store.exists("unknown-hash")) is False

    def test_duplicate_mark_is_idempotent(
        self, tracker_dir: str,
    ) -> None:
        store = FileCompletionStore(tracker_dir)

        async def _run():
            await store.mark("dup-hash")
            await store.mark("dup-hash")
            return await store.exists("dup-hash")

        assert asyncio.run(_run()) is True
        entries = os.listdir(tracker_dir)
        assert len(entries) == 1

    def test_cleanup_removes_old_records(
        self, tracker_dir: str,
    ) -> None:
        store = FileCompletionStore(tracker_dir)
        asyncio.run(store.mark("old-hash"))
        record_path = os.path.join(tracker_dir, "old-hash.complete")
        old_time = 0.0
        os.utime(record_path, (old_time, old_time))

        async def _run():
            removed = await store.cleanup(max_age_seconds=3600)
            exists = await store.exists("old-hash")
            return removed, exists

        removed, exists = asyncio.run(_run())
        assert removed >= 1
        assert exists is False


class TestCompletionTracker:
    def test_mark_and_check(self, tracker: CompletionTracker) -> None:
        async def _run():
            await tracker.mark_committed("content-hash-1")
            return await tracker.is_committed("content-hash-1")

        assert asyncio.run(_run()) is True

    def test_uncommitted_returns_false(
        self, tracker: CompletionTracker,
    ) -> None:
        assert asyncio.run(tracker.is_committed("never-committed")) is False

    def test_skip_already_committed(
        self, tracker: CompletionTracker,
    ) -> None:
        async def _run():
            await tracker.mark_committed("already-done")
            return await tracker.should_skip("already-done")

        assert asyncio.run(_run()) is True

    def test_do_not_skip_new(self, tracker: CompletionTracker) -> None:
        assert asyncio.run(tracker.should_skip("brand-new")) is False
