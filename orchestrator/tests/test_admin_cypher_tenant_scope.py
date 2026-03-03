"""Tests for admin Cypher tenant-scope enforcement."""
import pytest

from orchestrator.app.graph_api import enforce_cypher_tenant_scope


class TestCypherTenantScopeEnforcement:
    def test_rejects_cypher_without_tenant_param(self) -> None:
        with pytest.raises(ValueError, match="tenant_id"):
            enforce_cypher_tenant_scope("MATCH (n) RETURN n", "team-a")

    def test_allows_cypher_with_tenant_param(self) -> None:
        enforce_cypher_tenant_scope(
            "MATCH (n) WHERE n.tenant_id = $tenant_id RETURN n",
            "team-a",
        )

    def test_allows_wildcard_tenant_any_cypher(self) -> None:
        enforce_cypher_tenant_scope("MATCH (n) RETURN n", "*")

    def test_rejects_empty_tenant_id(self) -> None:
        with pytest.raises(ValueError, match="tenant_id"):
            enforce_cypher_tenant_scope("MATCH (n) RETURN n", "")

    def test_allows_tenant_param_in_where_clause(self) -> None:
        enforce_cypher_tenant_scope(
            "MATCH (n:Service) WHERE n.tenant_id = $tenant_id RETURN n.name",
            "acme-corp",
        )

    def test_rejects_partial_match_not_param(self) -> None:
        with pytest.raises(ValueError, match="tenant_id"):
            enforce_cypher_tenant_scope(
                "MATCH (n) WHERE n.some_tenant_id_field = 'x' RETURN n",
                "team-a",
            )
