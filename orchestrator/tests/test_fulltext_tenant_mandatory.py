import pytest

from orchestrator.app.query_engine import (
    FulltextTenantRequired,
    build_fulltext_fallback_cypher,
)


def test_fulltext_without_tenant_raises_always():
    with pytest.raises(FulltextTenantRequired):
        build_fulltext_fallback_cypher(tenant_id="")

    with pytest.raises(FulltextTenantRequired):
        build_fulltext_fallback_cypher(tenant_id="", deployment_mode="production")

    with pytest.raises(FulltextTenantRequired):
        build_fulltext_fallback_cypher(tenant_id="", deployment_mode="dev")


def test_fulltext_with_tenant_returns_scoped_query():
    cypher = build_fulltext_fallback_cypher(tenant_id="acme-corp")
    assert "$tenant_id" in cypher
    assert "node.tenant_id = $tenant_id" in cypher


def test_no_environment_bypass_exists():
    for mode in ("dev", "staging", "test", "local", ""):
        with pytest.raises(FulltextTenantRequired):
            build_fulltext_fallback_cypher(tenant_id="", deployment_mode=mode)
