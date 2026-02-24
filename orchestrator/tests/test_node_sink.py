from __future__ import annotations

import asyncio
from typing import Any, List

import pytest

from orchestrator.app.node_sink import IncrementalNodeSink, DurableNodeSink


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


class _FakeEntity:
    def __init__(self, name: str, namespace_acl: list | None = None, team_owner: str | None = None):
        self.name = name
        self.namespace_acl = namespace_acl
        self.team_owner = team_owner


class TestPartitionedWrites:

    def test_parallel_partitions_splits_by_namespace(self, repo: FakeRepository) -> None:
        sink = IncrementalNodeSink(repo, batch_size=4, parallel_partitions=True)
        entities = [
            _FakeEntity("svc-a", namespace_acl=["ns-alpha"]),
            _FakeEntity("svc-b", namespace_acl=["ns-beta"]),
            _FakeEntity("svc-c", namespace_acl=["ns-alpha"]),
            _FakeEntity("svc-d", namespace_acl=["ns-beta"]),
        ]

        asyncio.run(sink.ingest(entities))

        assert len(repo.committed_batches) == 2
        all_committed = [e for batch in repo.committed_batches for e in batch]
        names = {e.name for e in all_committed}
        assert names == {"svc-a", "svc-b", "svc-c", "svc-d"}

    def test_single_partition_does_not_split(self, repo: FakeRepository) -> None:
        sink = IncrementalNodeSink(repo, batch_size=3, parallel_partitions=True)
        entities = [
            _FakeEntity("svc-a", namespace_acl=["ns-alpha"]),
            _FakeEntity("svc-b", namespace_acl=["ns-alpha"]),
            _FakeEntity("svc-c", namespace_acl=["ns-alpha"]),
        ]
        asyncio.run(sink.ingest(entities))
        assert len(repo.committed_batches) == 1

    def test_fallback_to_team_owner(self, repo: FakeRepository) -> None:
        sink = IncrementalNodeSink(repo, batch_size=4, parallel_partitions=True)
        entities = [
            _FakeEntity("svc-a", team_owner="team-x"),
            _FakeEntity("svc-b", team_owner="team-y"),
            _FakeEntity("svc-c", team_owner="team-x"),
            _FakeEntity("svc-d", team_owner="team-y"),
        ]
        asyncio.run(sink.ingest(entities))
        assert len(repo.committed_batches) == 2

    def test_default_partition_key_groups_together(self, repo: FakeRepository) -> None:
        sink = IncrementalNodeSink(repo, batch_size=3, parallel_partitions=True)
        entities = [
            _FakeEntity("svc-a"),
            _FakeEntity("svc-b"),
            _FakeEntity("svc-c"),
        ]
        asyncio.run(sink.ingest(entities))
        assert len(repo.committed_batches) == 1

    def test_parallel_false_does_sequential(self, repo: FakeRepository) -> None:
        sink = IncrementalNodeSink(repo, batch_size=4, parallel_partitions=False)
        entities = [
            _FakeEntity("svc-a", namespace_acl=["ns-alpha"]),
            _FakeEntity("svc-b", namespace_acl=["ns-beta"]),
            _FakeEntity("svc-c", namespace_acl=["ns-alpha"]),
            _FakeEntity("svc-d", namespace_acl=["ns-beta"]),
        ]
        asyncio.run(sink.ingest(entities))
        assert len(repo.committed_batches) == 1

    def test_flush_with_partitions(self, repo: FakeRepository) -> None:
        sink = IncrementalNodeSink(repo, batch_size=100, parallel_partitions=True)
        entities = [
            _FakeEntity("svc-a", namespace_acl=["ns-alpha"]),
            _FakeEntity("svc-b", namespace_acl=["ns-beta"]),
        ]

        async def _run():
            await sink.ingest(entities)
            await sink.flush()

        asyncio.run(_run())
        assert len(repo.committed_batches) == 2
        assert sink.total_entities == 2


class FakeProducer:
    def __init__(self, fail_on_call: int = -1) -> None:
        self.messages: List[Any] = []
        self._call_count = 0
        self._fail_on_call = fail_on_call

    async def send(self, topic: str, value: bytes, key: bytes | None = None) -> None:
        self._call_count += 1
        if self._call_count == self._fail_on_call:
            raise RuntimeError("simulated Kafka failure")
        self.messages.append({"topic": topic, "value": value, "key": key})


class TestDurableNodeSink:

    def test_ingest_publishes_before_buffering(self) -> None:
        repo = FakeRepository()
        producer = FakeProducer()
        sink = DurableNodeSink(
            committer=repo,
            producer=producer,
            topic="entity-outbox",
            batch_size=10,
        )

        asyncio.run(sink.ingest(["entity-a", "entity-b"]))
        assert len(producer.messages) >= 1, (
            "DurableNodeSink must publish entities to Kafka before buffering"
        )

    def test_kafka_failure_prevents_buffering(self) -> None:
        repo = FakeRepository()
        producer = FakeProducer(fail_on_call=1)
        sink = DurableNodeSink(
            committer=repo,
            producer=producer,
            topic="entity-outbox",
            batch_size=10,
        )

        with pytest.raises(RuntimeError, match="simulated Kafka"):
            asyncio.run(sink.ingest(["entity-a"]))
        assert len(repo.committed_batches) == 0, (
            "If Kafka publish fails, entities must NOT be committed"
        )

    def test_flush_commits_and_tracks(self) -> None:
        repo = FakeRepository()
        producer = FakeProducer()
        sink = DurableNodeSink(
            committer=repo,
            producer=producer,
            topic="entity-outbox",
            batch_size=100,
        )

        async def _run():
            await sink.ingest(["a", "b", "c"])
            await sink.flush()

        asyncio.run(_run())
        assert sink.total_entities == 3
        assert len(repo.committed_batches) == 1

    def test_crash_mid_batch_data_survives_in_kafka(self) -> None:
        repo = FakeRepository()
        repo.fail_on_call = 1
        producer = FakeProducer()
        sink = DurableNodeSink(
            committer=repo,
            producer=producer,
            topic="entity-outbox",
            batch_size=2,
        )

        with pytest.raises(RuntimeError, match="simulated Neo4j"):
            asyncio.run(sink.ingest(["a", "b"]))

        assert len(producer.messages) >= 1, (
            "Even when Neo4j commit fails, entities must already be "
            "durably published to Kafka for crash recovery"
        )
