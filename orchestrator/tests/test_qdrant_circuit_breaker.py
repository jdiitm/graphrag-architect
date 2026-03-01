from __future__ import annotations

import pytest

from orchestrator.app.vector_store import QdrantVectorStore


class TestQdrantCircuitBreakerWiring:

    def test_qdrant_store_has_circuit_breaker(self) -> None:
        store = QdrantVectorStore(url="http://localhost:6333")
        assert hasattr(store, "_circuit_breaker"), (
            "QdrantVectorStore must have a circuit breaker to prevent "
            "cascading failures when Qdrant is unavailable"
        )
        assert store._circuit_breaker is not None

    def test_qdrant_store_accepts_custom_breaker(self) -> None:
        from orchestrator.app.circuit_breaker import (
            CircuitBreaker,
            CircuitBreakerConfig,
        )
        custom_cb = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=5),
            name="qdrant",
        )
        store = QdrantVectorStore(
            url="http://localhost:6333",
            circuit_breaker=custom_cb,
        )
        assert store._circuit_breaker is custom_cb
