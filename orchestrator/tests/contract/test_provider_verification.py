import base64
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError
from starlette.testclient import TestClient

from orchestrator.app.main import app
from orchestrator.tests.contract.contracts import (
    ENDPOINT_CONTRACTS,
    STATUS_CODE_RETRY_MAP,
    ErrorResponse,
    ExpectedAsyncResponse,
    ExpectedHealthResponse,
    ExpectedIngestResponse,
    GoIngestDocument,
    GoIngestRequest,
    RetryBehavior,
)


def _b64(text: str) -> str:
    return base64.b64encode(text.encode()).decode("ascii")


def _go_ingest_payload() -> dict:
    return GoIngestRequest(
        documents=[
            GoIngestDocument(
                file_path="svc/main.go",
                content=_b64("package main\nfunc main() {}"),
                source_type="source_code",
            ),
        ],
    ).model_dump()


def _go_ingest_payload_multi() -> dict:
    return GoIngestRequest(
        documents=[
            GoIngestDocument(
                file_path="svc/main.go",
                content=_b64("package main"),
                source_type="source_code",
                repository="myrepo",
                commit_sha="abc123",
            ),
            GoIngestDocument(
                file_path="k8s/deploy.yaml",
                content=_b64("apiVersion: apps/v1"),
                source_type="k8s_manifest",
            ),
        ],
    ).model_dump()


SUCCESSFUL_GRAPH_RESULT = {
    "commit_status": "committed",
    "extracted_nodes": [{"type": "Service", "name": "main"}],
    "extraction_errors": [],
}


@pytest.fixture(name="client")
def fixture_client():
    return TestClient(app, raise_server_exceptions=False)


class TestIngestSyncContract:
    @patch("orchestrator.app.main.ingestion_graph")
    def test_go_payload_accepted(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value=SUCCESSFUL_GRAPH_RESULT)
        response = client.post("/ingest?sync=true", json=_go_ingest_payload())
        assert response.status_code == 200

    @patch("orchestrator.app.main.ingestion_graph")
    def test_response_validates_against_contract(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value=SUCCESSFUL_GRAPH_RESULT)
        response = client.post("/ingest?sync=true", json=_go_ingest_payload())
        ExpectedIngestResponse.model_validate(response.json())

    @patch("orchestrator.app.main.ingestion_graph")
    def test_multi_doc_payload_accepted(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value=SUCCESSFUL_GRAPH_RESULT)
        response = client.post(
            "/ingest?sync=true", json=_go_ingest_payload_multi(),
        )
        assert response.status_code == 200

    @patch("orchestrator.app.main.ingestion_graph")
    def test_response_status_field_present(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value=SUCCESSFUL_GRAPH_RESULT)
        response = client.post("/ingest?sync=true", json=_go_ingest_payload())
        body = response.json()
        assert "status" in body
        assert isinstance(body["status"], str)

    @patch("orchestrator.app.main.ingestion_graph")
    def test_response_entities_extracted_is_int(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value=SUCCESSFUL_GRAPH_RESULT)
        response = client.post("/ingest?sync=true", json=_go_ingest_payload())
        body = response.json()
        assert "entities_extracted" in body
        assert isinstance(body["entities_extracted"], int)

    @patch("orchestrator.app.main.ingestion_graph")
    def test_response_errors_is_list(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value=SUCCESSFUL_GRAPH_RESULT)
        response = client.post("/ingest?sync=true", json=_go_ingest_payload())
        body = response.json()
        assert "errors" in body
        assert isinstance(body["errors"], list)


class TestIngestAsyncContract:
    @patch("orchestrator.app.main.ingestion_graph")
    def test_async_returns_202(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value=SUCCESSFUL_GRAPH_RESULT)
        response = client.post("/ingest", json=_go_ingest_payload())
        assert response.status_code == 202

    @patch("orchestrator.app.main.ingestion_graph")
    def test_async_response_validates_against_contract(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value=SUCCESSFUL_GRAPH_RESULT)
        response = client.post("/ingest", json=_go_ingest_payload())
        ExpectedAsyncResponse.model_validate(response.json())

    @patch("orchestrator.app.main.ingestion_graph")
    def test_async_response_has_job_id(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value=SUCCESSFUL_GRAPH_RESULT)
        response = client.post("/ingest", json=_go_ingest_payload())
        body = response.json()
        assert "job_id" in body
        assert isinstance(body["job_id"], str)
        assert len(body["job_id"]) > 0


class TestHealthContract:
    def test_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200

    def test_response_validates_against_contract(self, client):
        response = client.get("/health")
        ExpectedHealthResponse.model_validate(response.json())

    def test_status_field_is_healthy(self, client):
        response = client.get("/health")
        assert response.json()["status"] == "healthy"


class TestErrorResponseRetryContract:
    def test_validation_error_returns_422(self, client):
        response = client.post("/ingest?sync=true", json={})
        assert response.status_code == 422

    def test_invalid_base64_returns_400(self, client):
        payload = {
            "documents": [{
                "file_path": "f.go",
                "content": "not-valid-base64!!!",
                "source_type": "source_code",
            }],
        }
        response = client.post("/ingest?sync=true", json=payload)
        assert response.status_code == 400

    def test_400_error_has_detail(self, client):
        payload = {
            "documents": [{
                "file_path": "f.go",
                "content": "not-valid-base64!!!",
                "source_type": "source_code",
            }],
        }
        response = client.post("/ingest?sync=true", json=payload)
        body = response.json()
        assert "detail" in body

    def test_429_maps_to_retry(self):
        assert STATUS_CODE_RETRY_MAP[429] == RetryBehavior.RETRY

    def test_503_maps_to_retry(self):
        assert STATUS_CODE_RETRY_MAP[503] == RetryBehavior.RETRY

    def test_400_maps_to_dlq(self):
        assert STATUS_CODE_RETRY_MAP[400] == RetryBehavior.DLQ

    def test_422_maps_to_dlq(self):
        assert STATUS_CODE_RETRY_MAP[422] == RetryBehavior.DLQ

    def test_500_maps_to_dlq(self):
        assert STATUS_CODE_RETRY_MAP[500] == RetryBehavior.DLQ

    def test_413_maps_to_dlq(self):
        assert STATUS_CODE_RETRY_MAP[413] == RetryBehavior.DLQ


class TestFieldNameCompatibility:
    @patch("orchestrator.app.main.ingestion_graph")
    def test_ingest_response_snake_case_fields(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value=SUCCESSFUL_GRAPH_RESULT)
        response = client.post("/ingest?sync=true", json=_go_ingest_payload())
        body = response.json()
        assert "entities_extracted" in body
        assert "entitiesExtracted" not in body

    @patch("orchestrator.app.main.ingestion_graph")
    def test_ingest_request_snake_case_accepted(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value=SUCCESSFUL_GRAPH_RESULT)
        payload = {
            "documents": [{
                "file_path": "test.go",
                "content": _b64("package main"),
                "source_type": "source_code",
                "commit_sha": "abc123",
            }],
        }
        response = client.post("/ingest?sync=true", json=payload)
        assert response.status_code == 200

    def test_health_response_field_names(self, client):
        body = client.get("/health").json()
        assert set(body.keys()) == {"status"}


class TestContentTypeHeaders:
    @patch("orchestrator.app.main.ingestion_graph")
    def test_ingest_sync_json_content_type(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value=SUCCESSFUL_GRAPH_RESULT)
        response = client.post("/ingest?sync=true", json=_go_ingest_payload())
        assert "application/json" in response.headers["content-type"]

    def test_health_json_content_type(self, client):
        response = client.get("/health")
        assert "application/json" in response.headers["content-type"]

    def test_validation_error_json_content_type(self, client):
        response = client.post("/ingest?sync=true", json={})
        assert "application/json" in response.headers["content-type"]

    def test_400_error_json_content_type(self, client):
        payload = {
            "documents": [{
                "file_path": "f.go",
                "content": "not-valid-base64!!!",
                "source_type": "source_code",
            }],
        }
        response = client.post("/ingest?sync=true", json=payload)
        assert "application/json" in response.headers["content-type"]
