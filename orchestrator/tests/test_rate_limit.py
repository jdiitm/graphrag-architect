import base64
from unittest.mock import AsyncMock, patch

import pytest
from starlette.testclient import TestClient

from orchestrator.app.distributed_lock import LocalFallbackSemaphore
from orchestrator.app.main import app, set_ingestion_semaphore


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _ingest_payload() -> dict:
    return {
        "documents": [{
            "file_path": "main.go",
            "content": _b64("package main"),
            "source_type": "source_code",
        }],
    }


@pytest.fixture(name="client")
def fixture_client():
    return TestClient(app, raise_server_exceptions=False)


class TestRateLimitWithinLimit:
    @patch("orchestrator.app.main.ingestion_graph")
    def test_requests_within_limit_succeed(self, mock_graph, client):
        set_ingestion_semaphore(LocalFallbackSemaphore(max_concurrent=2))
        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "extraction_errors": [],
            "commit_status": "success",
        })
        response = client.post("/ingest?sync=true", json=_ingest_payload())
        assert response.status_code == 200
        set_ingestion_semaphore(None)


class TestRateLimitExceeded:
    @patch("orchestrator.app.main.ingestion_graph")
    def test_requests_exceeding_limit_get_429(self, mock_graph, client):
        sem = LocalFallbackSemaphore(max_concurrent=0)
        set_ingestion_semaphore(sem)

        response = client.post("/ingest?sync=true", json=_ingest_payload())
        assert response.status_code == 429
        assert "Too many concurrent" in response.json()["detail"]

        set_ingestion_semaphore(None)


class TestRateLimitResets:
    @patch("orchestrator.app.main.ingestion_graph")
    def test_counter_resets_after_request_completes(self, mock_graph, client):
        set_ingestion_semaphore(LocalFallbackSemaphore(max_concurrent=1))
        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "extraction_errors": [],
            "commit_status": "success",
        })

        first = client.post("/ingest?sync=true", json=_ingest_payload())
        assert first.status_code == 200

        second = client.post("/ingest?sync=true", json=_ingest_payload())
        assert second.status_code == 200
        set_ingestion_semaphore(None)
