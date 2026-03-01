from __future__ import annotations

import pytest

from orchestrator.app.query_engine import (
    FulltextTenantRequired,
    build_fulltext_fallback_cypher,
)


class TestFulltextTenantGuard:
    def test_tenant_scoped_includes_where_clause(self) -> None:
        cypher = build_fulltext_fallback_cypher(tenant_id="acme")
        assert "WHERE node.tenant_id = $tenant_id" in cypher

    def test_tenant_scoped_returns_ordered_limited(self) -> None:
        cypher = build_fulltext_fallback_cypher(tenant_id="acme")
        assert "ORDER BY score DESC LIMIT $limit" in cypher

    def test_empty_tenant_production_raises(self) -> None:
        with pytest.raises(FulltextTenantRequired):
            build_fulltext_fallback_cypher(
                tenant_id="", deployment_mode="production",
            )

    def test_empty_tenant_dev_returns_unscoped(self) -> None:
        cypher = build_fulltext_fallback_cypher(
            tenant_id="", deployment_mode="dev",
        )
        assert "service_name_index" in cypher
        assert "tenant_id" not in cypher

    def test_empty_tenant_defaults_to_dev(self) -> None:
        cypher = build_fulltext_fallback_cypher(tenant_id="")
        assert "service_name_index" in cypher

    def test_fulltext_index_name_consistent(self) -> None:
        cypher = build_fulltext_fallback_cypher(tenant_id="t1")
        assert "service_name_index" in cypher
