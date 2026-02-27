import asyncio
import base64
from unittest.mock import AsyncMock, patch

import pytest
from starlette.testclient import TestClient

from orchestrator.app.distributed_lock import LocalFallbackSemaphore
from orchestrator.app.main import app, set_ingestion_semaphore


@pytest.fixture(name="client")
def fixture_client():
    return TestClient(app, raise_server_exceptions=False)


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _valid_ingest_payload():
    return {
        "documents": [{
            "file_path": "main.go",
            "content": _b64("package main"),
            "source_type": "source_code",
        }],
    }


class TestAsyncIngestReturns202:
    @patch("orchestrator.app.main.ingestion_graph")
    def test_post_ingest_returns_202_with_job_id_and_pending_status(
        self, mock_graph, client
    ):
        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "extraction_errors": [],
            "commit_status": "success",
        })
        response = client.post("/ingest", json=_valid_ingest_payload())
        assert response.status_code == 202
        body = response.json()
        assert "job_id" in body
        assert body["status"] == "pending"


class TestGetIngestJobStatus:
    @patch("orchestrator.app.main.ingestion_graph")
    def test_get_existing_job_returns_status(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "extraction_errors": [],
            "commit_status": "success",
        })
        post_resp = client.post("/ingest", json=_valid_ingest_payload())
        job_id = post_resp.json()["job_id"]
        get_resp = client.get(f"/ingest/{job_id}")
        assert get_resp.status_code == 200
        body = get_resp.json()
        assert body["job_id"] == job_id
        assert body["status"] in ("pending", "running", "completed")

    def test_get_nonexistent_job_returns_404(self, client):
        response = client.get("/ingest/does-not-exist-id")
        assert response.status_code == 404


class TestCompletedIngestJob:
    @pytest.mark.asyncio
    @patch("orchestrator.app.main.ingestion_graph")
    async def test_completed_job_result_has_status_entities_and_errors(
        self, mock_graph
    ):
        from orchestrator.app.main import _INGEST_JOB_STORE, _run_ingest_job

        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": ["e1", "e2", "e3"],
            "extraction_errors": ["warn: partial parse"],
            "commit_status": "success",
        })
        job = await _INGEST_JOB_STORE.create()
        await _run_ingest_job(
            job.job_id,
            [{"path": "main.go", "content": "package main"}],
        )
        completed = await _INGEST_JOB_STORE.get(job.job_id)
        assert completed is not None
        assert completed.status.value == "completed"
        assert completed.result is not None
        assert completed.result.status == "success"
        assert completed.result.entities_extracted == 3
        assert completed.result.errors == ["warn: partial parse"]


class TestAsyncIngestSemaphoreGating:
    @pytest.mark.asyncio
    @patch("orchestrator.app.main.ingestion_graph")
    async def test_run_ingest_job_acquires_and_releases_semaphore(
        self, mock_graph
    ):
        from orchestrator.app.main import _INGEST_JOB_STORE, _run_ingest_job

        sem = LocalFallbackSemaphore(max_concurrent=1)
        set_ingestion_semaphore(sem)

        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "extraction_errors": [],
            "commit_status": "success",
        })
        job = await _INGEST_JOB_STORE.create()
        await _run_ingest_job(
            job.job_id,
            [{"path": "main.go", "content": "package main"}],
        )
        assert sem.active_count == 0
        set_ingestion_semaphore(None)

    @pytest.mark.asyncio
    @patch("orchestrator.app.main.ingestion_graph")
    async def test_run_ingest_job_releases_semaphore_on_failure(
        self, mock_graph
    ):
        from orchestrator.app.main import _INGEST_JOB_STORE, _run_ingest_job

        sem = LocalFallbackSemaphore(max_concurrent=1)
        set_ingestion_semaphore(sem)

        mock_graph.ainvoke = AsyncMock(
            side_effect=RuntimeError("graph exploded")
        )
        job = await _INGEST_JOB_STORE.create()
        await _run_ingest_job(
            job.job_id,
            [{"path": "main.go", "content": "package main"}],
        )
        assert sem.active_count == 0
        completed = await _INGEST_JOB_STORE.get(job.job_id)
        assert completed is not None
        assert completed.status.value == "failed"
        set_ingestion_semaphore(None)
