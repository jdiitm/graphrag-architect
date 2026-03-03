from __future__ import annotations

from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.extraction_models import ServiceNode
from orchestrator.app.node_sink import IncrementalNodeSink
from orchestrator.app.vector_sync_outbox import VectorSyncEvent


SAMPLE_SERVICE = ServiceNode(
    id="order-service",
    name="order-service",
    language="go",
    framework="gin",
    opentelemetry_enabled=True,
    tenant_id="test-tenant",
)


class _FakeCommitter:
    def __init__(self) -> None:
        self.committed_batches: List[List[Any]] = []
        self.outbox_events: List[List[Any]] = []

    async def commit_topology(self, entities: List[Any]) -> None:
        self.committed_batches.append(entities)

    async def commit_topology_with_outbox(
        self,
        entities: List[Any],
        outbox_events: Optional[List[Any]] = None,
    ) -> None:
        self.committed_batches.append(entities)
        self.outbox_events.append(outbox_events or [])


class _FailingOutboxCommitter:
    def __init__(self) -> None:
        self.committed_batches: List[List[Any]] = []

    async def commit_topology(self, entities: List[Any]) -> None:
        self.committed_batches.append(entities)

    async def commit_topology_with_outbox(
        self,
        entities: List[Any],
        outbox_events: Optional[List[Any]] = None,
    ) -> None:
        raise RuntimeError("outbox write failed inside tx")


class TestFlushWithOutbox:

    @pytest.mark.asyncio
    async def test_flush_with_outbox_passes_events_to_committer(self) -> None:
        committer = _FakeCommitter()
        sink = IncrementalNodeSink(committer, batch_size=100)
        await sink.ingest([SAMPLE_SERVICE])

        event = VectorSyncEvent(
            collection="svc_embeddings",
            pruned_ids=["old-node-1"],
            tenant_id="test-tenant",
        )
        await sink.flush_with_outbox([event])

        assert len(committer.outbox_events) == 1
        assert committer.outbox_events[0] == [event]

    @pytest.mark.asyncio
    async def test_flush_with_outbox_entities_still_committed(self) -> None:
        committer = _FakeCommitter()
        sink = IncrementalNodeSink(committer, batch_size=100)
        await sink.ingest([SAMPLE_SERVICE])

        await sink.flush_with_outbox([])

        assert len(committer.committed_batches) == 1
        assert any(
            e.id == "order-service" for e in committer.committed_batches[0]
        )

    @pytest.mark.asyncio
    async def test_outbox_failure_prevents_commit(self) -> None:
        committer = _FailingOutboxCommitter()
        sink = IncrementalNodeSink(committer, batch_size=100)
        await sink.ingest([SAMPLE_SERVICE])

        with pytest.raises(RuntimeError, match="outbox write failed"):
            await sink.flush_with_outbox([
                VectorSyncEvent(collection="svc", pruned_ids=["x"]),
            ])

        assert len(committer.committed_batches) == 0

    @pytest.mark.asyncio
    async def test_empty_buffer_with_outbox_is_noop(self) -> None:
        committer = _FakeCommitter()
        sink = IncrementalNodeSink(committer, batch_size=100)

        await sink.flush_with_outbox([
            VectorSyncEvent(collection="svc", pruned_ids=["x"]),
        ])

        assert len(committer.committed_batches) == 0
        assert len(committer.outbox_events) == 0
