import base64
from unittest.mock import AsyncMock, patch

import pytest
from starlette.testclient import TestClient

from orchestrator.app.access_control import sign_token
from orchestrator.app.main import app


@pytest.fixture(name="client")
def fixture_client():
    return TestClient(app, raise_server_exceptions=False)


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


class TestHealthEndpoint:
    def test_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200

    def test_returns_healthy_status(self, client):
        response = client.get("/health")
        body = response.json()
        assert body["status"] == "healthy"


class TestIngestRequestValidation:
    def test_missing_documents_field_returns_422(self, client):
        response = client.post("/ingest", json={})
        assert response.status_code == 422

    def test_empty_documents_list_returns_422(self, client):
        response = client.post("/ingest", json={"documents": []})
        assert response.status_code == 422

    def test_document_missing_file_path_returns_422(self, client):
        response = client.post("/ingest", json={
            "documents": [{"content": _b64("hello"), "source_type": "source_code"}],
        })
        assert response.status_code == 422

    def test_document_missing_content_returns_422(self, client):
        response = client.post("/ingest", json={
            "documents": [{"file_path": "main.go", "source_type": "source_code"}],
        })
        assert response.status_code == 422

    def test_document_missing_source_type_returns_422(self, client):
        response = client.post("/ingest", json={
            "documents": [{"file_path": "main.go", "content": _b64("hello")}],
        })
        assert response.status_code == 422

    def test_invalid_source_type_returns_422(self, client):
        response = client.post("/ingest", json={
            "documents": [{
                "file_path": "main.go",
                "content": _b64("hello"),
                "source_type": "invalid_type",
            }],
        })
        assert response.status_code == 422


class TestIngestBase64Decoding:
    @patch("orchestrator.app.main.ingestion_graph")
    def test_invalid_base64_returns_400(self, mock_graph, client):
        response = client.post("/ingest", json={
            "documents": [{
                "file_path": "main.go",
                "content": "not-valid-base64!!!@@@",
                "source_type": "source_code",
            }],
        })
        assert response.status_code == 400
        assert "base64" in response.json()["detail"].lower()

    @patch("orchestrator.app.main.ingestion_graph")
    def test_valid_base64_decoded_correctly(self, mock_graph, client):
        original = "package main\n\nfunc main() {}\n"
        encoded = _b64(original)
        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "extraction_errors": [],
            "commit_status": "success",
        })
        response = client.post("/ingest", json={
            "documents": [{
                "file_path": "main.go",
                "content": encoded,
                "source_type": "source_code",
            }],
        })
        assert response.status_code == 200
        call_args = mock_graph.ainvoke.call_args[0][0]
        assert call_args["raw_files"][0]["content"] == original


class TestIngestGraphInvocation:
    @patch("orchestrator.app.main.ingestion_graph")
    def test_invokes_graph_with_correct_state(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": ["node1", "node2"],
            "extraction_errors": [],
            "commit_status": "success",
        })
        go_content = _b64("package main")
        yaml_content = _b64("apiVersion: apps/v1\nkind: Deployment")
        response = client.post("/ingest", json={
            "documents": [
                {"file_path": "svc/main.go", "content": go_content, "source_type": "source_code"},
                {"file_path": "k8s/deploy.yaml", "content": yaml_content, "source_type": "k8s_manifest"},
            ],
        })
        assert response.status_code == 200
        call_args = mock_graph.ainvoke.call_args[0][0]
        assert call_args["directory_path"] == ""
        assert len(call_args["raw_files"]) == 2
        assert call_args["raw_files"][0]["path"] == "svc/main.go"
        assert call_args["raw_files"][1]["path"] == "k8s/deploy.yaml"
        assert call_args["extracted_nodes"] == []
        assert call_args["extraction_errors"] == []
        assert call_args["validation_retries"] == 0
        assert call_args["commit_status"] == ""

    @patch("orchestrator.app.main.ingestion_graph")
    def test_returns_success_response(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": ["n1", "n2", "n3"],
            "extraction_errors": [],
            "commit_status": "success",
        })
        response = client.post("/ingest", json={
            "documents": [
                {"file_path": "app.py", "content": _b64("print('hi')"), "source_type": "source_code"},
            ],
        })
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        assert body["entities_extracted"] == 3
        assert body["errors"] == []

    @patch("orchestrator.app.main.ingestion_graph")
    def test_returns_503_when_commit_fails(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "extraction_errors": ["Neo4j connection refused"],
            "commit_status": "failed",
        })
        response = client.post("/ingest", json={
            "documents": [
                {"file_path": "app.py", "content": _b64("x=1"), "source_type": "source_code"},
            ],
        })
        assert response.status_code == 503
        body = response.json()
        assert body["status"] == "failed"
        assert body["errors"] == ["Neo4j connection refused"]

    @patch("orchestrator.app.main.ingestion_graph")
    def test_graph_exception_returns_500(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(side_effect=RuntimeError("DAG crashed"))
        response = client.post("/ingest", json={
            "documents": [
                {"file_path": "app.py", "content": _b64("x=1"), "source_type": "source_code"},
            ],
        })
        assert response.status_code == 500
        assert "detail" in response.json()


class TestIngestOptionalFields:
    @patch("orchestrator.app.main.ingestion_graph")
    def test_repository_and_commit_sha_accepted(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "extraction_errors": [],
            "commit_status": "success",
        })
        response = client.post("/ingest", json={
            "documents": [{
                "file_path": "main.go",
                "content": _b64("package main"),
                "source_type": "source_code",
                "repository": "github.com/org/repo",
                "commit_sha": "abc123",
            }],
        })
        assert response.status_code == 200

    @patch("orchestrator.app.main.ingestion_graph")
    def test_optional_fields_can_be_omitted(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "extraction_errors": [],
            "commit_status": "success",
        })
        response = client.post("/ingest", json={
            "documents": [{
                "file_path": "main.go",
                "content": _b64("package main"),
                "source_type": "source_code",
            }],
        })
        assert response.status_code == 200


class TestIngestAuth:
    @patch("orchestrator.app.main.ingestion_graph")
    @patch.dict("os.environ", {"AUTH_TOKEN_SECRET": "test-secret"})
    def test_ingest_with_valid_token_succeeds(self, mock_graph, client):
        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "extraction_errors": [],
            "commit_status": "success",
        })
        token = sign_token("team=ops,role=admin", "test-secret")
        response = client.post(
            "/ingest",
            json={
                "documents": [{
                    "file_path": "main.go",
                    "content": _b64("package main"),
                    "source_type": "source_code",
                }],
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 200

    @patch("orchestrator.app.main.ingestion_graph")
    @patch.dict("os.environ", {"AUTH_TOKEN_SECRET": "test-secret"})
    def test_ingest_with_invalid_token_returns_401(self, mock_graph, client):
        response = client.post(
            "/ingest",
            json={
                "documents": [{
                    "file_path": "main.go",
                    "content": _b64("package main"),
                    "source_type": "source_code",
                }],
            },
            headers={"Authorization": "Bearer forged.deadbeef"},
        )
        assert response.status_code == 401

    @patch("orchestrator.app.main.ingestion_graph")
    @patch.dict("os.environ", {"AUTH_TOKEN_SECRET": "test-secret"})
    def test_ingest_without_token_when_secret_configured_returns_401(
        self, mock_graph, client
    ):
        response = client.post(
            "/ingest",
            json={
                "documents": [{
                    "file_path": "main.go",
                    "content": _b64("package main"),
                    "source_type": "source_code",
                }],
            },
        )
        assert response.status_code == 401

    @patch("orchestrator.app.main.ingestion_graph")
    @patch.dict("os.environ", {}, clear=False)
    def test_ingest_without_token_when_no_secret_allows_anonymous(
        self, mock_graph, client
    ):
        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "extraction_errors": [],
            "commit_status": "success",
        })
        import os
        os.environ.pop("AUTH_TOKEN_SECRET", None)
        response = client.post(
            "/ingest",
            json={
                "documents": [{
                    "file_path": "main.go",
                    "content": _b64("package main"),
                    "source_type": "source_code",
                }],
            },
        )
        assert response.status_code == 200


class TestAuthRequireTokensFailClosed:
    @patch("orchestrator.app.main.ingestion_graph")
    @patch.dict("os.environ", {"AUTH_REQUIRE_TOKENS": "true"}, clear=False)
    def test_ingest_rejects_when_require_tokens_and_no_secret(
        self, mock_graph, client
    ):
        import os
        os.environ.pop("AUTH_TOKEN_SECRET", None)
        response = client.post(
            "/ingest",
            json={
                "documents": [{
                    "file_path": "main.go",
                    "content": _b64("package main"),
                    "source_type": "source_code",
                }],
            },
        )
        assert response.status_code == 503

    @patch("orchestrator.app.main.ingestion_graph")
    @patch.dict(
        "os.environ",
        {"AUTH_REQUIRE_TOKENS": "true", "AUTH_TOKEN_SECRET": "prod-secret"},
        clear=False,
    )
    def test_ingest_works_when_require_tokens_and_secret_present(
        self, mock_graph, client
    ):
        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "extraction_errors": [],
            "commit_status": "success",
        })
        token = sign_token("team=ops,role=admin", "prod-secret")
        response = client.post(
            "/ingest",
            json={
                "documents": [{
                    "file_path": "main.go",
                    "content": _b64("package main"),
                    "source_type": "source_code",
                }],
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 200

    @patch("orchestrator.app.main.ingestion_graph")
    @patch.dict("os.environ", {}, clear=False)
    def test_ingest_allows_when_require_tokens_not_set(
        self, mock_graph, client
    ):
        mock_graph.ainvoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "extraction_errors": [],
            "commit_status": "success",
        })
        import os
        os.environ.pop("AUTH_TOKEN_SECRET", None)
        os.environ.pop("AUTH_REQUIRE_TOKENS", None)
        response = client.post(
            "/ingest",
            json={
                "documents": [{
                    "file_path": "main.go",
                    "content": _b64("package main"),
                    "source_type": "source_code",
                }],
            },
        )
        assert response.status_code == 200


class TestLoadWorkspacePreservesRawFiles:
    def test_empty_directory_preserves_existing_raw_files(self):
        from orchestrator.app.graph_builder import load_workspace_files

        existing_files = [
            {"path": "main.go", "content": "package main"},
            {"path": "deploy.yaml", "content": "kind: Deployment"},
        ]
        state = {
            "directory_path": "",
            "raw_files": existing_files,
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        result = load_workspace_files(state)
        assert result["raw_files"] == existing_files

    def test_missing_directory_path_preserves_raw_files(self):
        from orchestrator.app.graph_builder import load_workspace_files

        existing_files = [{"path": "app.py", "content": "x = 1"}]
        state = {
            "raw_files": existing_files,
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        result = load_workspace_files(state)
        assert result["raw_files"] == existing_files

    def test_directory_path_set_loads_from_directory(self, tmp_path):
        from orchestrator.app.graph_builder import load_workspace_files

        go_file = tmp_path / "service.go"
        go_file.write_text("package main", encoding="utf-8")

        state = {
            "directory_path": str(tmp_path),
            "raw_files": [],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        result = load_workspace_files(state)
        assert len(result["raw_files"]) == 1
        assert result["raw_files"][0]["path"] == "service.go"
