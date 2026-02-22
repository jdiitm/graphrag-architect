from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
)
from orchestrator.app.extraction_models import ServiceNode, CallsEdge
from orchestrator.app.neo4j_client import GraphRepository


def _mock_async_session(execute_write_side_effect=None):
    mock_session = AsyncMock()
    if execute_write_side_effect:
        mock_session.execute_write = AsyncMock(
            side_effect=execute_write_side_effect
        )

    mock_driver = MagicMock()

    @asynccontextmanager
    async def session_ctx(**kwargs):
        yield mock_session

    mock_driver.session = session_ctx
    mock_driver.close = AsyncMock()
    return mock_driver, mock_session


class TestGraphRepositoryWithCircuitBreaker:
    @pytest.mark.asyncio
    async def test_commit_succeeds_through_circuit_breaker(self):
        mock_driver, mock_session = _mock_async_session()
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=2))
        repo = GraphRepository(mock_driver, circuit_breaker=cb)

        entities = [
            ServiceNode(
                id="svc-1", name="auth", language="python",
                framework="fastapi", opentelemetry_enabled=True,
            ),
        ]
        await repo.commit_topology(entities)
        mock_session.execute_write.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_circuit_opens_after_repeated_failures(self):
        mock_driver, _ = _mock_async_session(
            execute_write_side_effect=OSError("conn refused")
        )
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=2))
        repo = GraphRepository(mock_driver, circuit_breaker=cb)

        entities = [
            ServiceNode(
                id="svc-1", name="auth", language="python",
                framework="fastapi", opentelemetry_enabled=True,
            ),
        ]

        for _ in range(2):
            with pytest.raises(OSError):
                await repo.commit_topology(entities)

        with pytest.raises(CircuitOpenError):
            await repo.commit_topology(entities)

    @pytest.mark.asyncio
    async def test_empty_entities_skips_circuit_breaker(self):
        mock_driver, mock_session = _mock_async_session()
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
            ),
        ]
        mock_topology.calls = [
            CallsEdge(
                source_service_id="svc-1",
                target_service_id="svc-2",
                protocol="grpc",
            ),
        ]

        mock_driver, mock_session = _mock_async_session()

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch("orchestrator.app.graph_builder.ServiceExtractor") as mock_ext,
            patch("orchestrator.app.graph_builder.AsyncGraphDatabase") as mock_neo4j,
        ):
            mock_extractor = AsyncMock()
            mock_extractor.extract_all = AsyncMock(return_value=mock_topology)
            mock_ext.return_value = mock_extractor
            mock_neo4j.driver.return_value = mock_driver

            result = await ingestion_graph.ainvoke({
                "directory_path": "",
                "raw_files": [
                    {"path": "auth/main.py", "content": "from fastapi import FastAPI"},
                ],
                "extracted_nodes": [],
                "extraction_errors": [],
                "validation_retries": 0,
                "commit_status": "",
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
            patch("orchestrator.app.graph_builder.AsyncGraphDatabase") as mock_neo4j,
        ):
            mock_extractor = AsyncMock()
            mock_extractor.extract_all = AsyncMock(return_value=mock_topology)
            mock_ext.return_value = mock_extractor
            mock_neo4j.driver.side_effect = OSError("connection refused")

            result = await ingestion_graph.ainvoke({
                "directory_path": "",
                "raw_files": [],
                "extracted_nodes": [],
                "extraction_errors": [],
                "validation_retries": 0,
                "commit_status": "",
            })

            assert result["commit_status"] == "failed"


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
            })

            assert result["complexity"].value == "entity_lookup"
            assert result["answer"] != ""
