from __future__ import annotations

import asyncio
import time

import pytest

from orchestrator.app.ingestion_resume import (
    IngestionStatusStore,
    IngestionStatus,
    InMemoryStatusStore,
)


@pytest.fixture()
def store() -> InMemoryStatusStore:
    return InMemoryStatusStore()


class TestIngestionStatus:
    def test_initial_status_is_running(self, store: InMemoryStatusStore) -> None:
        asyncio.run(store.create("thread-1", total_files=10))
        status = asyncio.run(store.get("thread-1"))
        assert status is not None
        assert status.state == "running"
        assert status.total_files == 10
        assert status.processed_files == 0

    def test_update_progress(self, store: InMemoryStatusStore) -> None:
        async def _run():
            await store.create("thread-2", total_files=5)
            await store.update_progress("thread-2", processed=3)
            return await store.get("thread-2")

        status = asyncio.run(_run())
        assert status is not None
        assert status.processed_files == 3

    def test_mark_completed(self, store: InMemoryStatusStore) -> None:
        async def _run():
            await store.create("thread-3", total_files=5)
            await store.mark_completed("thread-3")
            return await store.get("thread-3")

        status = asyncio.run(_run())
        assert status is not None
        assert status.state == "completed"

    def test_mark_failed(self, store: InMemoryStatusStore) -> None:
        async def _run():
            await store.create("thread-4", total_files=5)
            await store.mark_failed("thread-4", error="OOM killed")
            return await store.get("thread-4")

        status = asyncio.run(_run())
        assert status is not None
        assert status.state == "failed"
        assert status.error == "OOM killed"

    def test_get_nonexistent_returns_none(
        self, store: InMemoryStatusStore,
    ) -> None:
        status = asyncio.run(store.get("nonexistent"))
        assert status is None

    def test_cleanup_removes_old(self, store: InMemoryStatusStore) -> None:
        async def _run():
            await store.create("old-thread", total_files=1)
            await store.mark_completed("old-thread")
            status = await store.get("old-thread")
            status.completed_at = time.time() - 100000
            removed = await store.cleanup(max_age_seconds=3600)
            return removed, await store.get("old-thread")

        removed, after = asyncio.run(_run())
        assert removed >= 1
        assert after is None

    def test_list_resumable(self, store: InMemoryStatusStore) -> None:
        async def _run():
            await store.create("running-1", total_files=10)
            await store.create("failed-1", total_files=5)
            await store.mark_failed("failed-1", error="crash")
            await store.create("done-1", total_files=3)
            await store.mark_completed("done-1")
            return await store.list_resumable()

        resumable = asyncio.run(_run())
        thread_ids = [s.thread_id for s in resumable]
        assert "failed-1" in thread_ids
        assert "done-1" not in thread_ids

    def test_concurrent_creates_safe(
        self, store: InMemoryStatusStore,
    ) -> None:
        async def _run():
            tasks = [
                store.create(f"concurrent-{i}", total_files=1)
                for i in range(10)
            ]
            await asyncio.gather(*tasks)
            count = 0
            for i in range(10):
                if await store.get(f"concurrent-{i}") is not None:
                    count += 1
            return count

        assert asyncio.run(_run()) == 10
