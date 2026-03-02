from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.vector_sync_outbox import (
    CoalescingOutbox,
    VectorSyncEvent,
    VectorSyncRouter,
)


@pytest.fixture
def kafka_publisher():
    publisher = AsyncMock()
    publisher.publish = AsyncMock()
    return publisher


@pytest.fixture
def memory_outbox():
    return CoalescingOutbox()


class TestVectorSyncRouter:
    @pytest.mark.asyncio
    async def test_kafka_mode_publishes_mutation_event(
        self, kafka_publisher, memory_outbox,
    ):
        router = VectorSyncRouter(
            mode="kafka",
            kafka_publisher=kafka_publisher,
            memory_outbox=memory_outbox,
        )
        event = VectorSyncEvent(
            collection="service_embeddings",
            operation="upsert",
            vectors=[{"id": "svc-1", "embedding": [0.1]}],
        )
        await router.route(event)
        kafka_publisher.publish.assert_awaited_once()
        published_event = kafka_publisher.publish.call_args[0][0]
        assert published_event.collection == "service_embeddings"
        assert memory_outbox.pending_count == 0

    @pytest.mark.asyncio
    async def test_memory_mode_uses_coalescing_outbox(
        self, kafka_publisher, memory_outbox,
    ):
        router = VectorSyncRouter(
            mode="memory",
            kafka_publisher=kafka_publisher,
            memory_outbox=memory_outbox,
        )
        event = VectorSyncEvent(
            collection="service_embeddings",
            pruned_ids=["id-1"],
        )
        await router.route(event)
        assert memory_outbox.pending_count == 1
        kafka_publisher.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_kafka_fallback_to_memory_on_failure(
        self, kafka_publisher, memory_outbox, caplog,
    ):
        kafka_publisher.publish = AsyncMock(
            side_effect=ConnectionError("broker down"),
        )
        router = VectorSyncRouter(
            mode="kafka",
            kafka_publisher=kafka_publisher,
            memory_outbox=memory_outbox,
        )
        event = VectorSyncEvent(
            collection="service_embeddings",
            pruned_ids=["id-2"],
        )
        with caplog.at_level(logging.WARNING):
            await router.route(event)
        assert memory_outbox.pending_count == 1
        assert any("fallback" in r.message.lower() for r in caplog.records)

    def test_collection_name_consistency(self):
        from orchestrator.app.graph_builder import resolve_vector_collection
        from orchestrator.app.query_engine import _VECTOR_COLLECTION as _qe_collection

        builder_collection = resolve_vector_collection()
        assert builder_collection == "service_embeddings"
        assert _qe_collection == "service_embeddings"


class TestVectorSyncRouterFromEnv:
    def test_default_mode_is_memory(self, monkeypatch):
        monkeypatch.delenv("VECTOR_SYNC_MODE", raising=False)
        router = VectorSyncRouter.from_env(memory_outbox=CoalescingOutbox())
        assert router.mode == "memory"

    def test_kafka_mode_from_env(self, monkeypatch):
        monkeypatch.setenv("VECTOR_SYNC_MODE", "kafka")
        publisher = AsyncMock()
        router = VectorSyncRouter.from_env(
            memory_outbox=CoalescingOutbox(),
            kafka_publisher=publisher,
        )
        assert router.mode == "kafka"
