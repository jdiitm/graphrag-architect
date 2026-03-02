from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.vector_sync_outbox import (
    OutboxOverflowError,
    VectorSyncEvent,
    VectorSyncRouter,
)


class TestOutboxOverflowStrategy:
    @pytest.fixture
    def failing_kafka(self) -> AsyncMock:
        publisher = AsyncMock()
        publisher.publish = AsyncMock(
            side_effect=ConnectionError("kafka broker down"),
        )
        return publisher

    @pytest.fixture
    def memory_outbox(self) -> MagicMock:
        outbox = MagicMock()
        outbox.enqueue = MagicMock()
        return outbox

    @pytest.fixture
    def sample_event(self) -> VectorSyncEvent:
        return VectorSyncEvent(
            collection="test",
            operation="upsert",
            vectors=[{"id": "v1", "embedding": [0.1]}],
        )

    @pytest.mark.asyncio
    async def test_reject_strategy_raises_on_kafka_failure(
        self,
        failing_kafka: AsyncMock,
        memory_outbox: MagicMock,
        sample_event: VectorSyncEvent,
    ) -> None:
        router = VectorSyncRouter(
            mode="kafka",
            kafka_publisher=failing_kafka,
            memory_outbox=memory_outbox,
            overflow_strategy="reject",
        )
        with pytest.raises(OutboxOverflowError):
            await router.route(sample_event)
        memory_outbox.enqueue.assert_not_called()

    @pytest.mark.asyncio
    async def test_buffer_strategy_falls_back_to_memory(
        self,
        failing_kafka: AsyncMock,
        memory_outbox: MagicMock,
        sample_event: VectorSyncEvent,
    ) -> None:
        router = VectorSyncRouter(
            mode="kafka",
            kafka_publisher=failing_kafka,
            memory_outbox=memory_outbox,
            overflow_strategy="buffer",
        )
        await router.route(sample_event)
        memory_outbox.enqueue.assert_called_once_with(sample_event)

    @pytest.mark.asyncio
    async def test_default_buffer_when_not_specified(
        self,
        failing_kafka: AsyncMock,
        memory_outbox: MagicMock,
        sample_event: VectorSyncEvent,
    ) -> None:
        router = VectorSyncRouter(
            mode="kafka",
            kafka_publisher=failing_kafka,
            memory_outbox=memory_outbox,
        )
        await router.route(sample_event)
        memory_outbox.enqueue.assert_called_once()

    def test_from_env_picks_reject_strategy(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VECTOR_SYNC_OVERFLOW_STRATEGY", "reject")
        monkeypatch.setenv("VECTOR_SYNC_MODE", "kafka")
        router = VectorSyncRouter.from_env()
        assert router.overflow_strategy == "reject"

    def test_from_env_defaults_to_buffer(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("VECTOR_SYNC_OVERFLOW_STRATEGY", raising=False)
        monkeypatch.setenv("VECTOR_SYNC_MODE", "kafka")
        router = VectorSyncRouter.from_env()
        assert router.overflow_strategy == "buffer"

    @pytest.mark.asyncio
    async def test_successful_kafka_publish_no_overflow(
        self,
        memory_outbox: MagicMock,
        sample_event: VectorSyncEvent,
    ) -> None:
        good_kafka = AsyncMock()
        good_kafka.publish = AsyncMock()
        router = VectorSyncRouter(
            mode="kafka",
            kafka_publisher=good_kafka,
            memory_outbox=memory_outbox,
            overflow_strategy="reject",
        )
        await router.route(sample_event)
        good_kafka.publish.assert_called_once()
        memory_outbox.enqueue.assert_not_called()
