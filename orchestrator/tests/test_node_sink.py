from __future__ import annotations

import asyncio
from typing import Any, List

import pytest

from orchestrator.app.node_sink import IncrementalNodeSink


class FakeRepository:
    def __init__(self) -> None:
        self.committed_batches: List[List[Any]] = []
        self.fail_on_call: int = -1
        self._call_count = 0

    async def commit_topology(self, nodes: List[Any]) -> None:
        self._call_count += 1
        if self._call_count == self.fail_on_call:
            raise RuntimeError("simulated Neo4j failure")
        self.committed_batches.append(list(nodes))


@pytest.fixture()
def repo() -> FakeRepository:
    return FakeRepository()


class TestIncrementalNodeSink:
    def test_flush_at_threshold(self, repo: FakeRepository) -> None:
        sink = IncrementalNodeSink(repo, batch_size=3)
        asyncio.run(sink.ingest(["a", "b", "c"]))
        assert len(repo.committed_batches) == 1
        assert repo.committed_batches[0] == ["a", "b", "c"]

    def test_no_flush_below_threshold(self, repo: FakeRepository) -> None:
        sink = IncrementalNodeSink(repo, batch_size=5)
        asyncio.run(sink.ingest(["a", "b"]))
        assert len(repo.committed_batches) == 0

    def test_final_flush_commits_remainder(
        self, repo: FakeRepository,
    ) -> None:
        sink = IncrementalNodeSink(repo, batch_size=5)

        async def _run():
            await sink.ingest(["a", "b"])
            await sink.flush()

        asyncio.run(_run())
        assert len(repo.committed_batches) == 1
        assert repo.committed_batches[0] == ["a", "b"]

    def test_multiple_flushes_at_threshold(
        self, repo: FakeRepository,
    ) -> None:
        sink = IncrementalNodeSink(repo, batch_size=2)

        async def _run():
            await sink.ingest(["a", "b", "c", "d", "e"])
            await sink.flush()

        asyncio.run(_run())
        total_committed = sum(len(b) for b in repo.committed_batches)
        assert total_committed == 5

    def test_empty_flush_is_noop(self, repo: FakeRepository) -> None:
        sink = IncrementalNodeSink(repo, batch_size=10)
        asyncio.run(sink.flush())
        assert len(repo.committed_batches) == 0

    def test_sink_failure_propagates(self, repo: FakeRepository) -> None:
        repo.fail_on_call = 1
        sink = IncrementalNodeSink(repo, batch_size=2)
        with pytest.raises(RuntimeError, match="simulated Neo4j failure"):
            asyncio.run(sink.ingest(["a", "b"]))

    def test_entity_count_tracked(self, repo: FakeRepository) -> None:
        sink = IncrementalNodeSink(repo, batch_size=3)

        async def _run():
            await sink.ingest(["a", "b", "c"])
            await sink.ingest(["d"])
            await sink.flush()

        asyncio.run(_run())
        assert sink.total_entities == 4
        assert sink.flush_count == 2

    def test_large_input_stays_bounded(self, repo: FakeRepository) -> None:
        sink = IncrementalNodeSink(repo, batch_size=10)

        async def _run():
            for _ in range(100):
                await sink.ingest(list(range(10)))
            await sink.flush()

        asyncio.run(_run())
        assert sink.total_entities == 1000
        assert len(sink._buffer) == 0
