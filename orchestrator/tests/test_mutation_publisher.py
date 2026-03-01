from __future__ import annotations

import json
from typing import Any, Dict, List
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.mutation_publisher import (
    GraphMutationEvent,
    KafkaMutationPublisher,
)


class TestGraphMutationEventModel:

    def test_required_fields_present(self) -> None:
        event = GraphMutationEvent(
            mutation_type="node_upsert",
            entity_ids=["svc-auth", "svc-pay"],
            tenant_id="acme",
        )
        assert event.event_id
        assert event.mutation_type == "node_upsert"
        assert event.entity_ids == ["svc-auth", "svc-pay"]
        assert event.tenant_id == "acme"
        assert event.timestamp

    def test_event_id_auto_generated_unique(self) -> None:
        a = GraphMutationEvent(
            mutation_type="node_upsert", entity_ids=["x"],
        )
        b = GraphMutationEvent(
            mutation_type="node_upsert", entity_ids=["x"],
        )
        assert a.event_id != b.event_id

    def test_mutation_type_node_upsert(self) -> None:
        event = GraphMutationEvent(
            mutation_type="node_upsert", entity_ids=["a"],
        )
        assert event.mutation_type == "node_upsert"

    def test_mutation_type_edge_upsert(self) -> None:
        event = GraphMutationEvent(
            mutation_type="edge_upsert", entity_ids=["a"],
        )
        assert event.mutation_type == "edge_upsert"

    def test_mutation_type_edge_tombstone(self) -> None:
        event = GraphMutationEvent(
            mutation_type="edge_tombstone", entity_ids=["a"],
        )
        assert event.mutation_type == "edge_tombstone"

    def test_mutation_type_node_delete(self) -> None:
        event = GraphMutationEvent(
            mutation_type="node_delete", entity_ids=["a"],
        )
        assert event.mutation_type == "node_delete"

    def test_invalid_mutation_type_rejected(self) -> None:
        with pytest.raises(Exception):
            GraphMutationEvent(
                mutation_type="invalid_op", entity_ids=["a"],
            )

    def test_serializes_to_json(self) -> None:
        event = GraphMutationEvent(
            mutation_type="edge_tombstone",
            entity_ids=["svc-1", "svc-2"],
            tenant_id="t1",
        )
        payload = event.model_dump()
        raw = json.dumps(payload)
        parsed = json.loads(raw)
        assert parsed["mutation_type"] == "edge_tombstone"
        assert parsed["entity_ids"] == ["svc-1", "svc-2"]
        assert parsed["tenant_id"] == "t1"
        assert "event_id" in parsed
        assert "timestamp" in parsed

    def test_tenant_id_defaults_to_empty(self) -> None:
        event = GraphMutationEvent(
            mutation_type="node_upsert", entity_ids=["a"],
        )
        assert event.tenant_id == ""


class _FakeTransport:

    def __init__(self) -> None:
        self.published: List[tuple] = []
        self.should_fail = False

    async def publish(
        self, topic: str, events: List[Dict[str, Any]],
    ) -> None:
        if self.should_fail:
            raise ConnectionError("Kafka broker unreachable")
        self.published.append((topic, events))


class TestKafkaMutationPublisher:

    @pytest.mark.asyncio
    async def test_publishes_events_via_transport(self) -> None:
        transport = _FakeTransport()
        publisher = KafkaMutationPublisher(
            transport=transport, topic="graph.mutations",
        )
        events = [
            GraphMutationEvent(
                mutation_type="node_upsert",
                entity_ids=["svc-auth"],
                tenant_id="acme",
            ),
        ]
        await publisher.publish(events)
        assert len(transport.published) == 1
        topic, payloads = transport.published[0]
        assert topic == "graph.mutations"
        assert len(payloads) == 1
        assert payloads[0]["mutation_type"] == "node_upsert"

    @pytest.mark.asyncio
    async def test_handles_transport_failure_gracefully(self) -> None:
        transport = _FakeTransport()
        transport.should_fail = True
        publisher = KafkaMutationPublisher(
            transport=transport, topic="graph.mutations",
        )
        events = [
            GraphMutationEvent(
                mutation_type="edge_tombstone", entity_ids=["e1"],
            ),
        ]
        await publisher.publish(events)

    @pytest.mark.asyncio
    async def test_batches_multiple_events(self) -> None:
        transport = _FakeTransport()
        publisher = KafkaMutationPublisher(
            transport=transport, topic="graph.mutations",
        )
        events = [
            GraphMutationEvent(
                mutation_type="node_upsert", entity_ids=[f"svc-{i}"],
            )
            for i in range(5)
        ]
        await publisher.publish(events)
        assert len(transport.published) == 1
        _, payloads = transport.published[0]
        assert len(payloads) == 5

    @pytest.mark.asyncio
    async def test_empty_event_list_is_noop(self) -> None:
        transport = _FakeTransport()
        publisher = KafkaMutationPublisher(
            transport=transport, topic="graph.mutations",
        )
        await publisher.publish([])
        assert len(transport.published) == 0
