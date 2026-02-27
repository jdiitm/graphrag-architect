from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient


_VALID_INGEST_BODY = {
    "documents": [{
        "file_path": "test.py",
        "content": "dGVzdA==",
        "source_type": "source_code",
    }]
}


class TestIngestCircuitBreakerReturns503:
    def test_returns_503_with_retry_after_when_circuit_open(self) -> None:
        from orchestrator.app.circuit_breaker import CircuitOpenError
        from orchestrator.app.main import app

        client = TestClient(app)

        with patch(
            "orchestrator.app.main.ingestion_graph",
        ) as mock_graph:
            mock_graph.ainvoke = AsyncMock(
                side_effect=CircuitOpenError("circuit is open; retry after 30s"),
            )
            response = client.post(
                "/ingest?sync=true", json=_VALID_INGEST_BODY,
            )

        assert response.status_code == 503
        assert "Retry-After" in response.headers
        retry_val = int(response.headers["Retry-After"])
        assert retry_val > 0

    def test_returns_429_when_semaphore_locked(self) -> None:
        from orchestrator.app.distributed_lock import LocalFallbackSemaphore
        from orchestrator.app.main import app

        client = TestClient(app)

        locked_sem = LocalFallbackSemaphore(max_concurrent=0)

        with patch(
            "orchestrator.app.main.get_ingestion_semaphore",
            return_value=locked_sem,
        ):
            response = client.post(
                "/ingest?sync=true", json=_VALID_INGEST_BODY,
            )

        assert response.status_code == 429
