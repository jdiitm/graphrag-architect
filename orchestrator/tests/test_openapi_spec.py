import pytest
from starlette.testclient import TestClient

from orchestrator.app.main import app


@pytest.fixture(name="client")
def fixture_client():
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(name="openapi_schema")
def fixture_openapi_schema(client):
    response = client.get("/openapi.json")
    assert response.status_code == 200
    return response.json()


class TestOpenAPISchemaValidity:
    def test_openapi_json_returns_200(self, client):
        response = client.get("/openapi.json")
        assert response.status_code == 200

    def test_schema_version_is_3_1(self, openapi_schema):
        assert openapi_schema["openapi"].startswith("3.1")

    def test_schema_has_info_title(self, openapi_schema):
        assert openapi_schema["info"]["title"] == "GraphRAG Orchestrator"


class TestOpenAPIEndpointCoverage:
    def test_includes_legacy_health(self, openapi_schema):
        assert "/health" in openapi_schema["paths"]

    def test_includes_ingest(self, openapi_schema):
        assert "/ingest" in openapi_schema["paths"]

    def test_includes_query(self, openapi_schema):
        assert "/query" in openapi_schema["paths"]

    def test_includes_metrics(self, openapi_schema):
        assert "/metrics" in openapi_schema["paths"]

    def test_includes_health_live(self, openapi_schema):
        assert "/v1/health/live" in openapi_schema["paths"]

    def test_includes_health_ready(self, openapi_schema):
        assert "/v1/health/ready" in openapi_schema["paths"]


class TestOpenAPIEndpointDescriptions:
    def test_health_live_has_description(self, openapi_schema):
        live_op = openapi_schema["paths"]["/v1/health/live"]["get"]
        assert "description" in live_op or "summary" in live_op

    def test_health_ready_has_description(self, openapi_schema):
        ready_op = openapi_schema["paths"]["/v1/health/ready"]["get"]
        assert "description" in ready_op or "summary" in ready_op

    def test_ingest_has_description(self, openapi_schema):
        ingest_op = openapi_schema["paths"]["/ingest"]["post"]
        assert "description" in ingest_op or "summary" in ingest_op

    def test_query_has_description(self, openapi_schema):
        query_op = openapi_schema["paths"]["/query"]["post"]
        assert "description" in query_op or "summary" in query_op


class TestOpenAPIResponseSchemas:
    def test_health_live_has_response_schema(self, openapi_schema):
        live_get = openapi_schema["paths"]["/v1/health/live"]["get"]
        responses = live_get["responses"]
        assert "200" in responses

    def test_health_ready_has_200_and_503(self, openapi_schema):
        ready_get = openapi_schema["paths"]["/v1/health/ready"]["get"]
        responses = ready_get["responses"]
        assert "200" in responses
        assert "503" in responses


class TestOpenAPITags:
    def test_health_endpoints_tagged(self, openapi_schema):
        live_op = openapi_schema["paths"]["/v1/health/live"]["get"]
        ready_op = openapi_schema["paths"]["/v1/health/ready"]["get"]
        assert "tags" in live_op
        assert "tags" in ready_op
        assert "health" in live_op["tags"]
        assert "health" in ready_op["tags"]

    def test_ingest_endpoint_tagged(self, openapi_schema):
        ingest_op = openapi_schema["paths"]["/ingest"]["post"]
        assert "tags" in ingest_op

    def test_query_endpoint_tagged(self, openapi_schema):
        query_op = openapi_schema["paths"]["/query"]["post"]
        assert "tags" in query_op
