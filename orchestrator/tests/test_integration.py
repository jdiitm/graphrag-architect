import base64
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.testclient import TestClient

from orchestrator.app.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
)
from orchestrator.app.extraction_models import ServiceNode, CallsEdge
from orchestrator.app.main import app
from orchestrator.app.neo4j_client import GraphRepository
from orchestrator.tests.conftest import mock_async_session


class TestGraphRepositoryWithCircuitBreaker:
    @pytest.mark.asyncio
    async def test_commit_succeeds_through_circuit_breaker(self):
        mock_driver, mock_session = mock_async_session()
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=2))
        repo = GraphRepository(mock_driver, circuit_breaker=cb)

        entities = [
            ServiceNode(
                id="svc-1", name="auth", language="python",
                framework="fastapi", opentelemetry_enabled=True,
                tenant_id="test-tenant",
            ),
        ]
        await repo.commit_topology(entities)
        assert mock_session.execute_write.await_count == 2

    @pytest.mark.asyncio
    async def test_circuit_opens_after_repeated_failures(self):
        mock_driver, _ = mock_async_session(
            execute_write_side_effect=OSError("conn refused")
        )
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=2))
        repo = GraphRepository(mock_driver, circuit_breaker=cb)

        entities = [
            ServiceNode(
                id="svc-1", name="auth", language="python",
                framework="fastapi", opentelemetry_enabled=True,
                tenant_id="test-tenant",
            ),
        ]

        for _ in range(2):
            with pytest.raises(OSError):
                await repo.commit_topology(entities)

        with pytest.raises(CircuitOpenError):
            await repo.commit_topology(entities)

    @pytest.mark.asyncio
    async def test_empty_entities_skips_circuit_breaker(self):
        mock_driver, mock_session = mock_async_session()
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1))
        repo = GraphRepository(mock_driver, circuit_breaker=cb)

        await repo.commit_topology([])
        mock_session.execute_write.assert_not_awaited()


_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


class TestIngestionDAGFlow:
    @pytest.mark.asyncio
    async def test_full_dag_with_mocked_externals(self):
        from orchestrator.app.graph_builder import ingestion_graph

        mock_topology = MagicMock()
        mock_topology.services = [
            ServiceNode(
                id="svc-1", name="auth", language="python",
                framework="fastapi", opentelemetry_enabled=True,
                tenant_id="test-tenant",
            ),
        ]
        mock_topology.calls = [
            CallsEdge(
                source_service_id="svc-1",
                target_service_id="svc-2",
                protocol="grpc",
                tenant_id="test-tenant",
            ),
        ]

        mock_driver, mock_session = mock_async_session()

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch("orchestrator.app.graph_builder.ServiceExtractor") as mock_ext,
            patch("orchestrator.app.graph_builder.get_driver", return_value=mock_driver),
        ):
            mock_extractor = AsyncMock()
            mock_extractor.extract_all = AsyncMock(return_value=mock_topology)
            mock_ext.return_value = mock_extractor

            result = await ingestion_graph.ainvoke({
                "directory_path": "",
                "raw_files": [
                    {"path": "auth/main.py", "content": "from fastapi import FastAPI"},
                ],
                "extracted_nodes": [],
                "extraction_errors": [],
                "validation_retries": 0,
                "commit_status": "",
                "tenant_id": "test-tenant",
            })

            assert result["commit_status"] == "success"
            mock_extractor.extract_all.assert_awaited()
            mock_session.execute_write.assert_awaited()

    @pytest.mark.asyncio
    async def test_dag_handles_neo4j_failure_gracefully(self):
        from orchestrator.app.graph_builder import ingestion_graph

        mock_topology = MagicMock()
        mock_topology.services = []
        mock_topology.calls = []

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch("orchestrator.app.graph_builder.ServiceExtractor") as mock_ext,
            patch(
                "orchestrator.app.graph_builder.get_driver",
                side_effect=OSError("connection refused"),
            ),
        ):
            mock_extractor = AsyncMock()
            mock_extractor.extract_all = AsyncMock(return_value=mock_topology)
            mock_ext.return_value = mock_extractor

            result = await ingestion_graph.ainvoke({
                "directory_path": "",
                "raw_files": [],
                "extracted_nodes": [],
                "extraction_errors": [],
                "validation_retries": 0,
                "commit_status": "",
                "tenant_id": "test-tenant",
            })

            assert result["commit_status"] == "failed"


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


class TestHTTPThroughDAGIntegration:
    @pytest.fixture(name="client")
    def fixture_client(self):
        return TestClient(app, raise_server_exceptions=False)

    def test_post_ingest_runs_full_dag_and_commits(self, client):
        mock_topology = MagicMock()
        mock_topology.services = [
            ServiceNode(
                id="svc-1", name="auth", language="python",
                framework="fastapi", opentelemetry_enabled=True,
                tenant_id="test-tenant",
            ),
        ]
        mock_topology.calls = [
            CallsEdge(
                source_service_id="svc-1",
                target_service_id="svc-2",
                protocol="grpc",
                tenant_id="test-tenant",
            ),
        ]

        mock_driver, mock_session = mock_async_session()

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch("orchestrator.app.graph_builder.ServiceExtractor") as mock_ext,
            patch("orchestrator.app.graph_builder.get_driver", return_value=mock_driver),
        ):
            mock_extractor = AsyncMock()
            mock_extractor.extract_all = AsyncMock(return_value=mock_topology)
            mock_ext.return_value = mock_extractor

            response = client.post("/ingest?sync=true", json={
                "documents": [
                    {
                        "file_path": "auth/main.py",
                        "content": _b64("from fastapi import FastAPI"),
                        "source_type": "source_code",
                    },
                ],
            })

            assert response.status_code == 200
            body = response.json()
            assert body["status"] == "success"
            assert body["entities_extracted"] > 0
            mock_extractor.extract_all.assert_awaited()
            mock_session.execute_write.assert_awaited()

    def test_post_ingest_neo4j_failure_returns_503(self, client):
        mock_topology = MagicMock()
        mock_topology.services = []
        mock_topology.calls = []

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch("orchestrator.app.graph_builder.ServiceExtractor") as mock_ext,
            patch(
                "orchestrator.app.graph_builder.get_driver",
                side_effect=OSError("connection refused"),
            ),
        ):
            mock_extractor = AsyncMock()
            mock_extractor.extract_all = AsyncMock(return_value=mock_topology)
            mock_ext.return_value = mock_extractor

            response = client.post("/ingest?sync=true", json={
                "documents": [
                    {
                        "file_path": "app.py",
                        "content": _b64("x = 1"),
                        "source_type": "source_code",
                    },
                ],
            })

            assert response.status_code == 503
            assert response.json()["status"] == "failed"


class TestQueryDAGRouting:
    @pytest.mark.asyncio
    async def test_entity_lookup_routes_to_vector(self):
        from orchestrator.app.query_engine import query_graph

        mock_result = MagicMock()
        mock_result.data = MagicMock(return_value=[{"n.name": "auth-service"}])

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)

        mock_driver = MagicMock()

        @asynccontextmanager
        async def session_ctx(**kwargs):
            yield mock_session

        mock_driver.session = session_ctx
        mock_driver.close = AsyncMock()

        mock_llm = MagicMock()
        mock_llm.ainvoke = AsyncMock(
            return_value=MagicMock(content="Auth service is written in Python.")
        )

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch("orchestrator.app.query_engine._get_neo4j_driver", return_value=mock_driver),
            patch("orchestrator.app.query_engine.ChatGoogleGenerativeAI", return_value=mock_llm),
        ):
            result = await query_graph.ainvoke({
                "query": "What language is auth-service written in?",
                "complexity": "",
                "context_docs": [],
                "cypher_query": "",
                "cypher_result": [],
                "answer": "",
                "max_results": 10,
                "iteration_count": 0,
                "authorization": "",
            })

            assert result["complexity"].value == "entity_lookup"
            assert result["answer"] != ""
