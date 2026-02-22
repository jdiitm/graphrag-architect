from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.app.main import app
from orchestrator.app.query_models import QueryComplexity


@pytest.fixture
def client():
    return TestClient(app)


class TestQueryEndpointValidation:
    def test_missing_query_returns_422(self, client):
        response = client.post("/query", json={})
        assert response.status_code == 422

    def test_empty_query_returns_422(self, client):
        response = client.post("/query", json={"query": ""})
        assert response.status_code == 422


class TestQueryEndpointSuccess:
    def test_returns_200_with_answer(self, client):
        mock_result = {
            "query": "What language is auth-service?",
            "max_results": 10,
            "complexity": QueryComplexity.ENTITY_LOOKUP,
            "retrieval_path": "vector",
            "candidates": [{"name": "auth-service", "language": "go"}],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "auth-service is written in Go.",
            "sources": [{"name": "auth-service", "language": "go"}],
        }

        mock_graph = MagicMock()
        mock_graph.ainvoke = AsyncMock(return_value=mock_result)

        with patch("orchestrator.app.main.query_graph", mock_graph):
            response = client.post(
                "/query", json={"query": "What language is auth-service?"}
            )

        assert response.status_code == 200
        body = response.json()
        assert "answer" in body
        assert "sources" in body
        assert "complexity" in body
        assert body["answer"] == "auth-service is written in Go."

    def test_returns_complexity_and_path(self, client):
        mock_result = {
            "query": "blast radius of auth",
            "max_results": 10,
            "complexity": QueryComplexity.MULTI_HOP,
            "retrieval_path": "cypher",
            "candidates": [],
            "cypher_query": "MATCH ...",
            "cypher_results": [{"a": "b"}],
            "iteration_count": 1,
            "answer": "The blast radius includes user-service.",
            "sources": [{"a": "b"}],
        }

        mock_graph = MagicMock()
        mock_graph.ainvoke = AsyncMock(return_value=mock_result)

        with patch("orchestrator.app.main.query_graph", mock_graph):
            response = client.post(
                "/query", json={"query": "blast radius of auth"}
            )

        body = response.json()
        assert body["complexity"] == "multi_hop"
        assert body["retrieval_path"] == "cypher"


class TestQueryEndpointCustomMaxResults:
    def test_max_results_passed_through(self, client):
        mock_result = {
            "query": "services",
            "max_results": 3,
            "complexity": QueryComplexity.ENTITY_LOOKUP,
            "retrieval_path": "vector",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "Found services.",
            "sources": [],
        }

        mock_graph = MagicMock()
        mock_graph.ainvoke = AsyncMock(return_value=mock_result)

        with patch("orchestrator.app.main.query_graph", mock_graph):
            response = client.post(
                "/query", json={"query": "services", "max_results": 3}
            )

        assert response.status_code == 200
        call_state = mock_graph.ainvoke.call_args[0][0]
        assert call_state["max_results"] == 3


class TestQueryEndpointError:
    def test_graph_exception_returns_500(self, client):
        mock_graph = MagicMock()
        mock_graph.ainvoke = AsyncMock(
            side_effect=RuntimeError("Neo4j unavailable")
        )

        with patch("orchestrator.app.main.query_graph", mock_graph):
            response = client.post(
                "/query", json={"query": "What is auth-service?"}
            )

        assert response.status_code == 500
        assert "detail" in response.json()
