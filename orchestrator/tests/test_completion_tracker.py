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
        asyncio.get_event_loop().run_until_complete(
            store.mark("abc123")
        )
        entries = os.listdir(tracker_dir)
        assert len(entries) == 1

    def test_is_complete_returns_true_after_mark(
        self, tracker_dir: str,
    ) -> None:
        store = FileCompletionStore(tracker_dir)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(store.mark("hash-001"))
        result = loop.run_until_complete(store.exists("hash-001"))
        assert result is True

    def test_is_complete_returns_false_for_unknown(
        self, tracker_dir: str,
    ) -> None:
        store = FileCompletionStore(tracker_dir)
        result = asyncio.get_event_loop().run_until_complete(
            store.exists("unknown-hash")
        )
        assert result is False

    def test_duplicate_mark_is_idempotent(
        self, tracker_dir: str,
    ) -> None:
        store = FileCompletionStore(tracker_dir)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(store.mark("dup-hash"))
        loop.run_until_complete(store.mark("dup-hash"))
        result = loop.run_until_complete(store.exists("dup-hash"))
        assert result is True
        entries = os.listdir(tracker_dir)
        assert len(entries) == 1

    def test_cleanup_removes_old_records(
        self, tracker_dir: str,
    ) -> None:
        store = FileCompletionStore(tracker_dir)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(store.mark("old-hash"))
        record_path = os.path.join(tracker_dir, "old-hash.complete")
        old_time = 0.0
        os.utime(record_path, (old_time, old_time))
        removed = loop.run_until_complete(store.cleanup(max_age_seconds=3600))
        assert removed >= 1
        result = loop.run_until_complete(store.exists("old-hash"))
        assert result is False


class TestCompletionTracker:
    def test_mark_and_check(self, tracker: CompletionTracker) -> None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(tracker.mark_committed("content-hash-1"))
        result = loop.run_until_complete(
            tracker.is_committed("content-hash-1")
        )
        assert result is True

    def test_uncommitted_returns_false(
        self, tracker: CompletionTracker,
    ) -> None:
        result = asyncio.get_event_loop().run_until_complete(
            tracker.is_committed("never-committed")
        )
        assert result is False

    def test_skip_already_committed(
        self, tracker: CompletionTracker,
    ) -> None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(tracker.mark_committed("already-done"))
        should_skip = loop.run_until_complete(
            tracker.should_skip("already-done")
        )
        assert should_skip is True

    def test_do_not_skip_new(self, tracker: CompletionTracker) -> None:
        should_skip = asyncio.get_event_loop().run_until_complete(
            tracker.should_skip("brand-new")
        )
        assert should_skip is False
