from __future__ import annotations

import json
import os
from typing import Any, Dict, List
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.mutation_publisher import GraphMutationEvent
from orchestrator.app.vector_sync_consumer import VectorSyncKafkaConsumer
from orchestrator.app.config import VectorSyncConfig


class _FakeVectorDeleter:

    def __init__(self) -> None:
        self.deleted_ids: List[List[str]] = []

    async def delete(self, ids: List[str]) -> None:
        self.deleted_ids.append(ids)


class _FakeConsumerTransport:

    def __init__(self) -> None:
        self.events: List[Dict[str, Any]] = []


class TestVectorSyncKafkaConsumerEdgeTombstone:

    @pytest.mark.asyncio
    async def test_edge_tombstone_calls_vector_delete(self) -> None:
        deleter = _FakeVectorDeleter()
        transport = _FakeConsumerTransport()
        consumer = VectorSyncKafkaConsumer(
            transport=transport, vector_deleter=deleter,
        )
        event = GraphMutationEvent(
            mutation_type="edge_tombstone",
            entity_ids=["edge-1", "edge-2"],
            tenant_id="acme",
        )
        await consumer.process_event(event)
        assert len(deleter.deleted_ids) == 1
        assert deleter.deleted_ids[0] == ["edge-1", "edge-2"]

    @pytest.mark.asyncio
    async def test_node_delete_calls_vector_delete(self) -> None:
        deleter = _FakeVectorDeleter()
        transport = _FakeConsumerTransport()
        consumer = VectorSyncKafkaConsumer(
            transport=transport, vector_deleter=deleter,
        )
        event = GraphMutationEvent(
            mutation_type="node_delete",
            entity_ids=["node-1"],
        )
        await consumer.process_event(event)
        assert len(deleter.deleted_ids) == 1
        assert deleter.deleted_ids[0] == ["node-1"]


class TestVectorSyncKafkaConsumerNodeUpsert:

    @pytest.mark.asyncio
    async def test_node_upsert_is_noop_for_deleter(self) -> None:
        deleter = _FakeVectorDeleter()
        transport = _FakeConsumerTransport()
        consumer = VectorSyncKafkaConsumer(
            transport=transport, vector_deleter=deleter,
        )
        event = GraphMutationEvent(
            mutation_type="node_upsert",
            entity_ids=["svc-1"],
        )
        await consumer.process_event(event)
        assert len(deleter.deleted_ids) == 0

    @pytest.mark.asyncio
    async def test_edge_upsert_is_noop_for_deleter(self) -> None:
        deleter = _FakeVectorDeleter()
        transport = _FakeConsumerTransport()
        consumer = VectorSyncKafkaConsumer(
            transport=transport, vector_deleter=deleter,
        )
        event = GraphMutationEvent(
            mutation_type="edge_upsert",
            entity_ids=["edge-x"],
        )
        await consumer.process_event(event)
        assert len(deleter.deleted_ids) == 0


class TestVectorSyncKafkaConsumerMalformedEvents:

    @pytest.mark.asyncio
    async def test_malformed_event_logs_warning_continues(self) -> None:
        deleter = _FakeVectorDeleter()
        transport = _FakeConsumerTransport()
        consumer = VectorSyncKafkaConsumer(
            transport=transport, vector_deleter=deleter,
        )
        raw_payload = {"bad_key": "no mutation_type"}
        await consumer.process_raw(raw_payload)
        assert consumer.processed_count == 0

    @pytest.mark.asyncio
    async def test_valid_event_increments_processed_count(self) -> None:
        deleter = _FakeVectorDeleter()
        transport = _FakeConsumerTransport()
        consumer = VectorSyncKafkaConsumer(
            transport=transport, vector_deleter=deleter,
        )
        event = GraphMutationEvent(
            mutation_type="edge_tombstone",
            entity_ids=["e1"],
        )
        await consumer.process_event(event)
        assert consumer.processed_count == 1

    @pytest.mark.asyncio
    async def test_metrics_accumulate_across_events(self) -> None:
        deleter = _FakeVectorDeleter()
        transport = _FakeConsumerTransport()
        consumer = VectorSyncKafkaConsumer(
            transport=transport, vector_deleter=deleter,
        )
        for i in range(3):
            event = GraphMutationEvent(
                mutation_type="edge_tombstone",
                entity_ids=[f"e{i}"],
            )
            await consumer.process_event(event)
        assert consumer.processed_count == 3


class TestVectorSyncConfig:

    def test_from_env_defaults(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            cfg = VectorSyncConfig.from_env()
        assert cfg.backend == "memory"
        assert cfg.kafka_topic == "graph.mutations"

    def test_from_env_reads_kafka_backend(self) -> None:
        env = {
            "VECTOR_SYNC_BACKEND": "kafka",
            "VECTOR_SYNC_KAFKA_TOPIC": "custom.topic",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = VectorSyncConfig.from_env()
        assert cfg.backend == "kafka"
        assert cfg.kafka_topic == "custom.topic"

    def test_from_env_reads_memory_backend(self) -> None:
        env = {"VECTOR_SYNC_BACKEND": "memory"}
        with patch.dict(os.environ, env, clear=True):
            cfg = VectorSyncConfig.from_env()
        assert cfg.backend == "memory"

    def test_from_env_reads_redis_backend(self) -> None:
        env = {"VECTOR_SYNC_BACKEND": "redis"}
        with patch.dict(os.environ, env, clear=True):
            cfg = VectorSyncConfig.from_env()
        assert cfg.backend == "redis"

    def test_from_env_reads_neo4j_backend(self) -> None:
        env = {"VECTOR_SYNC_BACKEND": "neo4j"}
        with patch.dict(os.environ, env, clear=True):
            cfg = VectorSyncConfig.from_env()
        assert cfg.backend == "neo4j"
