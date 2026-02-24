from __future__ import annotations

from typing import Any, Dict, List, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.extraction_models import CallsEdge, ServiceNode

_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


def _make_services(count: int) -> List[ServiceNode]:
    return [
        ServiceNode(
            id=f"svc-{i}", name=f"service-{i}", language="go",
            framework="gin", opentelemetry_enabled=False, confidence=1.0,
        )
        for i in range(count)
    ]


class TestEdgeMergeStampsIngestionId:

    def test_calls_edge_cypher_includes_ingestion_id(self) -> None:
        from orchestrator.app.neo4j_client import _calls_cypher

        edge = CallsEdge(
            source_service_id="svc-a",
            target_service_id="svc-b",
            protocol="grpc",
            confidence=1.0,
        )
        query, params = _calls_cypher(edge)
        assert "ingestion_id" in query, (
            "CALLS edge MERGE must SET r.ingestion_id for tombstoning"
        )
        assert "last_seen_at" in query, (
            "CALLS edge MERGE must SET r.last_seen_at for age-based pruning"
        )

    def test_unwind_calls_edge_includes_ingestion_id(self) -> None:
        from orchestrator.app.neo4j_client import _UNWIND_QUERIES, CallsEdge

        query = _UNWIND_QUERIES[CallsEdge]
        assert "ingestion_id" in query, (
            "UNWIND CALLS edge query must include ingestion_id"
        )
        assert "last_seen_at" in query, (
            "UNWIND CALLS edge query must include last_seen_at"
        )


class TestPruneStaleEdges:

    def test_graph_repository_has_prune_method(self) -> None:
        from orchestrator.app.neo4j_client import GraphRepository
        assert hasattr(GraphRepository, "prune_stale_edges"), (
            "GraphRepository must have prune_stale_edges() method"
        )

    @pytest.mark.asyncio
    async def test_prune_executes_delete_query(self) -> None:
        from orchestrator.app.neo4j_client import GraphRepository

        executed_queries: List[str] = []

        mock_session = AsyncMock()

        async def _capture_write(fn, **kwargs):
            query = kwargs.get("query", "")
            executed_queries.append(query)

        mock_session.execute_write = _capture_write

        mock_driver = MagicMock()
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def session_ctx(**kwargs):
            yield mock_session

        mock_driver.session = session_ctx

        repo = GraphRepository(mock_driver)
        result = await repo.prune_stale_edges(
            current_ingestion_id="run-123",
            max_age_hours=24,
        )

        assert len(executed_queries) >= 1, (
            "prune_stale_edges must execute at least one DELETE query"
        )
        delete_query = executed_queries[0]
        assert "DELETE" in delete_query.upper(), (
            f"Prune query must contain DELETE, got: {delete_query}"
        )

    @pytest.mark.asyncio
    async def test_prune_called_after_successful_commit(self) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        prune_called = {"count": 0}

        async def _tracking_commit(self_repo, entities):
            pass

        async def _tracking_prune(self_repo, current_ingestion_id="", max_age_hours=24):
            prune_called["count"] += 1
            return 0

        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()
        entities = _make_services(3)

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.get_driver",
                return_value=mock_driver,
            ),
            patch(
                "orchestrator.app.neo4j_client.GraphRepository.commit_topology",
                _tracking_commit,
            ),
            patch(
                "orchestrator.app.neo4j_client.GraphRepository.prune_stale_edges",
                _tracking_prune,
            ),
        ):
            result = await commit_to_neo4j({"extracted_nodes": entities})

        assert result["commit_status"] == "success"
        assert prune_called["count"] >= 1, (
            "commit_to_neo4j must call prune_stale_edges after successful commit"
        )
