from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
)
from orchestrator.app.vector_store import (
    QdrantVectorStore,
    VectorDegradedError,
    VectorRecord,
)


class TestQdrantWriteTimeout:

    @pytest.mark.asyncio
    async def test_upsert_times_out(self) -> None:
        async def slow_upsert(**kwargs: Any) -> None:
            await asyncio.sleep(100)

        mock_client = MagicMock()
        mock_client.upsert = slow_upsert

        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=10,
                recovery_timeout=300.0,
                jitter_factor=0.0,
            ),
            name="test-upsert-timeout",
        )
        store = QdrantVectorStore(
            circuit_breaker=breaker,
            write_timeout_seconds=0.1,
        )
        store._client = mock_client

        records = [VectorRecord(id="r1", vector=[0.1, 0.2], metadata={})]
        with pytest.raises(VectorDegradedError):
            await store.upsert("test_collection", records)

    @pytest.mark.asyncio
    async def test_delete_times_out(self) -> None:
        async def slow_delete(**kwargs: Any) -> None:
            await asyncio.sleep(100)

        mock_client = MagicMock()
        mock_client.delete = slow_delete

        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=10,
                recovery_timeout=300.0,
                jitter_factor=0.0,
            ),
            name="test-delete-timeout",
        )
        store = QdrantVectorStore(
            circuit_breaker=breaker,
            write_timeout_seconds=0.1,
        )
        store._client = mock_client

        with pytest.raises(VectorDegradedError):
            await store.delete("test_collection", ["id1", "id2"])

    @pytest.mark.asyncio
    async def test_upsert_trips_circuit_breaker(self) -> None:
        mock_client = MagicMock()
        mock_client.upsert = AsyncMock(
            side_effect=ConnectionError("connection refused"),
        )

        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=300.0,
                jitter_factor=0.0,
            ),
            name="test-upsert-cb",
        )
        store = QdrantVectorStore(
            circuit_breaker=breaker,
            write_timeout_seconds=5.0,
        )
        store._client = mock_client

        records = [VectorRecord(id="r1", vector=[0.1, 0.2], metadata={})]
        for _ in range(3):
            with pytest.raises(ConnectionError):
                await store.upsert("test_collection", records)

        with pytest.raises(CircuitOpenError):
            await store.upsert("test_collection", records)
