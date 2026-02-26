from __future__ import annotations

import asyncio
import os
import random
from typing import Any, List
from unittest.mock import MagicMock, patch

import pytest

from orchestrator.app.extraction_models import (
    KafkaTopicNode,
    ServiceNode,
)
from orchestrator.app.neo4j_client import GraphRepository
from orchestrator.app.node_sink import IncrementalNodeSink


class _OrderCapturingRepo:
    def __init__(self) -> None:
        self.committed_batches: List[List[Any]] = []

    async def commit_topology(self, nodes: List[Any]) -> None:
        self.committed_batches.append(list(nodes))


def _make_service(entity_id: str, tenant: str = "t1") -> ServiceNode:
    return ServiceNode(
        id=entity_id,
        name=entity_id,
        language="go",
        framework="gin",
        opentelemetry_enabled=True,
        tenant_id=tenant,
    )


class _BareEntity:
    def __init__(self, value: int) -> None:
        self.value = value

    def __str__(self) -> str:
        return f"entity-{self.value:03d}"


class TestSinkSortsEntitiesByPrimaryKey:

    def test_sorts_by_id_before_commit(self) -> None:
        repo = _OrderCapturingRepo()
        sink = IncrementalNodeSink(repo, batch_size=3)
        entities = [
            _make_service("charlie"),
            _make_service("alpha"),
            _make_service("bravo"),
        ]
        asyncio.run(sink.ingest(entities))
        ids = [e.id for e in repo.committed_batches[0]]
        assert ids == ["alpha", "bravo", "charlie"]

    def test_sorts_by_name_when_no_id_field(self) -> None:
        repo = _OrderCapturingRepo()
        sink = IncrementalNodeSink(repo, batch_size=3)
        entities = [
            KafkaTopicNode(
                name="gamma-topic", partitions=3,
                retention_ms=1000, tenant_id="t1",
            ),
            KafkaTopicNode(
                name="alpha-topic", partitions=3,
                retention_ms=1000, tenant_id="t1",
            ),
            KafkaTopicNode(
                name="beta-topic", partitions=3,
                retention_ms=1000, tenant_id="t1",
            ),
        ]
        asyncio.run(sink.ingest(entities))
        names = [e.name for e in repo.committed_batches[0]]
        assert names == ["alpha-topic", "beta-topic", "gamma-topic"]

    def test_flush_path_also_sorts(self) -> None:
        repo = _OrderCapturingRepo()
        sink = IncrementalNodeSink(repo, batch_size=100)
        entities = [_make_service("charlie"), _make_service("alpha")]

        async def _run() -> None:
            await sink.ingest(entities)
            await sink.flush()

        asyncio.run(_run())
        ids = [e.id for e in repo.committed_batches[0]]
        assert ids == ["alpha", "charlie"]


class TestSortDeterminism:

    def test_deterministic_regardless_of_input_order(self) -> None:
        base = [_make_service(f"svc-{i:03d}") for i in range(10)]
        results: List[List[str]] = []
        for _ in range(5):
            shuffled = list(base)
            random.shuffle(shuffled)
            repo = _OrderCapturingRepo()
            sink = IncrementalNodeSink(repo, batch_size=10)
            asyncio.run(sink.ingest(shuffled))
            results.append([e.id for e in repo.committed_batches[0]])
        for result in results[1:]:
            assert result == results[0]


class TestSortInPartitionedPath:

    def test_sorts_within_single_partition(self) -> None:
        repo = _OrderCapturingRepo()
        sink = IncrementalNodeSink(
            repo, batch_size=3, parallel_partitions=True,
        )
        entities = [
            _make_service("charlie"),
            _make_service("alpha"),
            _make_service("bravo"),
        ]
        for e in entities:
            e.namespace_acl = ["ns1"]
        asyncio.run(sink.ingest(entities))
        ids = [e.id for e in repo.committed_batches[0]]
        assert ids == ["alpha", "bravo", "charlie"]

    def test_sorts_within_each_partition(self) -> None:
        repo = _OrderCapturingRepo()
        sink = IncrementalNodeSink(
            repo, batch_size=4, parallel_partitions=True,
        )
        entities = [
            ServiceNode(
                id="charlie", name="charlie", language="go",
                framework="gin", opentelemetry_enabled=True,
                tenant_id="t1", namespace_acl=["ns1"],
            ),
            ServiceNode(
                id="delta", name="delta", language="go",
                framework="gin", opentelemetry_enabled=True,
                tenant_id="t1", namespace_acl=["ns2"],
            ),
            ServiceNode(
                id="bravo", name="bravo", language="go",
                framework="gin", opentelemetry_enabled=True,
                tenant_id="t1", namespace_acl=["ns1"],
            ),
            ServiceNode(
                id="alpha", name="alpha", language="go",
                framework="gin", opentelemetry_enabled=True,
                tenant_id="t1", namespace_acl=["ns2"],
            ),
        ]
        asyncio.run(sink.ingest(entities))
        assert len(repo.committed_batches) == 2
        for batch in repo.committed_batches:
            ids = [e.id for e in batch]
            assert ids == sorted(ids)


class TestSortWithNoIdOrName:

    def test_entities_without_id_or_name_sorted_by_str(self) -> None:
        repo = _OrderCapturingRepo()
        sink = IncrementalNodeSink(repo, batch_size=4)
        entities = [
            _BareEntity(3), _BareEntity(1),
            _BareEntity(2), _BareEntity(0),
        ]
        asyncio.run(sink.ingest(entities))
        strs = [str(e) for e in repo.committed_batches[0]]
        assert strs == [
            "entity-000", "entity-001", "entity-002", "entity-003",
        ]


class TestWriteConcurrencyEnvVar:

    def test_env_var_overrides_default(self) -> None:
        with patch.dict(os.environ, {"WRITE_CONCURRENCY": "8"}):
            repo = GraphRepository(driver=MagicMock())
            assert repo._write_concurrency == 8

    def test_default_without_env_var(self) -> None:
        env = {k: v for k, v in os.environ.items() if k != "WRITE_CONCURRENCY"}
        with patch.dict(os.environ, env, clear=True):
            repo = GraphRepository(driver=MagicMock())
            assert repo._write_concurrency == 4

    def test_explicit_param_takes_precedence_over_env(self) -> None:
        with patch.dict(os.environ, {"WRITE_CONCURRENCY": "8"}):
            repo = GraphRepository(driver=MagicMock(), write_concurrency=2)
            assert repo._write_concurrency == 2
