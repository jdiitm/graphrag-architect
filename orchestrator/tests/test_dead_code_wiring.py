from __future__ import annotations

import os
import tempfile
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


class TestScopedEntityIdInMergeOps:

    def test_ast_extraction_produces_scoped_ids(self) -> None:
        from orchestrator.app.ast_extraction import GoASTExtractor

        files = [
            {"path": "services/auth/main.go", "content": (
                "package main\n"
                "import \"net/http\"\n"
                "func main() {\n"
                "  http.ListenAndServe(\":8080\", nil)\n"
                "}\n"
            )},
        ]
        extractor = GoASTExtractor()
        result = extractor.extract_all(files)
        if result.services:
            svc_id = result.services[0].service_id
            assert "::" in svc_id, (
                f"Service ID '{svc_id}' is not scoped. Expected "
                "'repository::namespace::name' format from ScopedEntityId."
            )

    def test_service_node_id_uses_scoped_format(self) -> None:
        from orchestrator.app.entity_resolver import resolve_entity_id

        scoped = resolve_entity_id("auth-svc", repository="myrepo", namespace="services")
        assert "::" in scoped
        assert scoped == "myrepo::services::auth-svc"


class TestCallIsolationWiredIntoACL:

    def test_ast_injection_covers_call_subquery_match(self) -> None:
        from orchestrator.app.access_control import CypherPermissionFilter, SecurityPrincipal

        principal = SecurityPrincipal(
            team="team-a",
            namespace="ns-1",
            role="viewer",
        )
        pf = CypherPermissionFilter(principal)
        cypher_with_call = (
            "MATCH (n:Service) WHERE n.team_owner = 'team-a' "
            "CALL { MATCH (m:Service) RETURN m } RETURN n"
        )
        result, params = pf.inject_into_cypher(cypher_with_call)

        subquery_start = result.find("CALL")
        subquery_end = result.find("}", subquery_start)
        subquery_body = result[subquery_start:subquery_end]
        assert "n.team_owner" in subquery_body, (
            "AST-level injection must cover MATCH clauses inside CALL subqueries"
        )


class TestCompletionTrackerWiredIntoCommit:

    @pytest.mark.asyncio
    async def test_commit_records_completion(self) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        topology_calls: List[List[Any]] = []

        async def _tracking_commit(self_repo, entities):
            topology_calls.append(list(entities))

        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()

        from orchestrator.app.extraction_models import ServiceNode
        entities = [
            ServiceNode(
                id="svc-1", name="auth", language="go",
                framework="gin", opentelemetry_enabled=False, confidence=1.0,
                tenant_id="test-tenant",
            ),
        ]

        completion_marks: List[str] = []

        async def _mock_mark(content_hash: str) -> None:
            completion_marks.append(content_hash)

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
        ):
            result = await commit_to_neo4j({"extracted_nodes": entities})

        assert result["commit_status"] == "success"
        assert len(completion_marks) > 0 or result.get("completion_tracked"), (
            "commit_to_neo4j must call CompletionTracker.mark_committed() "
            "after successful Neo4j commit."
        )
