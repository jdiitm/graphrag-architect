import pytest
from opentelemetry.sdk.metrics import MeterProvider
from starlette.testclient import TestClient

from orchestrator.app.main import app
from orchestrator.app.observability import configure_metrics


@pytest.fixture(name="client")
def fixture_client():
    return TestClient(app, raise_server_exceptions=False)


class TestConfigureMetrics:
    def test_returns_meter_provider(self):
        provider = configure_metrics()
        assert isinstance(provider, MeterProvider)

    def test_provider_has_metric_readers(self):
        provider = configure_metrics()
        readers = provider._all_metric_readers
        reader_types = [type(r).__name__ for r in readers]
        assert "PrometheusMetricReader" in reader_types


class TestMetricsEndpoint:
    def test_returns_200(self, client):
        response = client.get("/metrics")
        assert response.status_code == 200

    def test_content_type_is_prometheus(self, client):
        response = client.get("/metrics")
        assert "text/plain" in response.headers["content-type"]

    def test_contains_runtime_metrics(self, client):
        response = client.get("/metrics")
        assert "python_gc_objects_collected_total" in response.text


class TestHistogramsExportedViaEndpoint:
    def test_query_duration_in_metrics_output(self, client):
        from orchestrator.app.observability import QUERY_DURATION

        QUERY_DURATION.record(150.0)
        response = client.get("/metrics")
        assert "query_duration_ms" in response.text

    def test_ingestion_duration_in_metrics_output(self, client):
        from orchestrator.app.observability import INGESTION_DURATION

        INGESTION_DURATION.record(50.0)
        response = client.get("/metrics")
        assert "ingestion_duration_ms" in response.text

    def test_llm_extraction_duration_in_metrics_output(self, client):
        from orchestrator.app.observability import LLM_EXTRACTION_DURATION

        LLM_EXTRACTION_DURATION.record(200.0)
        response = client.get("/metrics")
        assert "llm_extraction_duration_ms" in response.text

    def test_neo4j_transaction_duration_in_metrics_output(self, client):
        from orchestrator.app.observability import NEO4J_TRANSACTION_DURATION

        NEO4J_TRANSACTION_DURATION.record(25.0)
        response = client.get("/metrics")
        assert "neo4j_transaction_duration_ms" in response.text
