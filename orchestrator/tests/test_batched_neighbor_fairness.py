from __future__ import annotations

from orchestrator.app.tenant_security import build_traversal_batched_neighbor


class TestBatchedNeighborPerSourceLimit:

    def test_query_contains_per_source_ordering(self) -> None:
        query = build_traversal_batched_neighbor()
        normalized = " ".join(query.split()).upper()
        assert "WITH" in normalized, (
            "Batched neighbor query must use WITH clause for per-source "
            "result limiting to prevent high-degree source domination"
        )

    def test_query_limits_results_per_source(self) -> None:
        query = build_traversal_batched_neighbor()
        normalized = " ".join(query.split()).lower()
        has_per_source = (
            "collect" in normalized
            or "$per_source_limit" in normalized
            or ("with" in normalized and "source" in normalized)
        )
        assert has_per_source, (
            "Batched neighbor query must enforce per-source result limits. "
            "A single global LIMIT allows high-degree nodes to dominate. "
            f"Query: {query}"
        )

    def test_query_still_has_global_limit(self) -> None:
        query = build_traversal_batched_neighbor()
        assert "LIMIT" in query.upper(), (
            "Query must still include a global LIMIT as a safety cap"
        )

    def test_query_still_has_acl(self) -> None:
        query = build_traversal_batched_neighbor()
        assert "$is_admin" in query
        assert "team_owner" in query
        assert "namespace_acl" in query

    def test_query_still_has_tenant_filter(self) -> None:
        query = build_traversal_batched_neighbor()
        assert "$tenant_id" in query
        assert "tenant_id" in query

    def test_query_still_has_tombstone_filter(self) -> None:
        query = build_traversal_batched_neighbor()
        assert "tombstoned_at IS NULL" in query
