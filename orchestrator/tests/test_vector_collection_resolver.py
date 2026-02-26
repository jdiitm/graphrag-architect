from orchestrator.app.graph_builder import resolve_vector_collection
from orchestrator.app.vector_store import resolve_collection_name

_BASE_COLLECTION = "services"


class TestResolveCollectionName:
    def test_empty_tenant_returns_base(self):
        assert resolve_collection_name(_BASE_COLLECTION, "") == "services"

    def test_none_tenant_returns_base(self):
        assert resolve_collection_name(_BASE_COLLECTION, None) == "services"

    def test_tenant_returns_prefixed(self):
        result = resolve_collection_name(_BASE_COLLECTION, "acme")
        assert result == "services__acme"

    def test_tenant_with_special_chars_sanitized(self):
        result = resolve_collection_name(_BASE_COLLECTION, "org/team-1")
        assert "/" not in result
        assert result.startswith("services__")

    def test_different_tenants_produce_different_names(self):
        a = resolve_collection_name(_BASE_COLLECTION, "tenant_a")
        b = resolve_collection_name(_BASE_COLLECTION, "tenant_b")
        assert a != b

    def test_same_tenant_is_deterministic(self):
        first = resolve_collection_name(_BASE_COLLECTION, "stable")
        second = resolve_collection_name(_BASE_COLLECTION, "stable")
        assert first == second

    def test_collision_resistance_slash_vs_underscore(self):
        a = resolve_collection_name(_BASE_COLLECTION, "org/team")
        b = resolve_collection_name(_BASE_COLLECTION, "org_team")
        assert a != b, "org/team and org_team must not collide"

    def test_collision_resistance_dot_vs_underscore(self):
        a = resolve_collection_name(_BASE_COLLECTION, "org.team")
        b = resolve_collection_name(_BASE_COLLECTION, "org_team")
        assert a != b, "org.team and org_team must not collide"

    def test_hyphen_preserved_literally(self):
        result = resolve_collection_name(_BASE_COLLECTION, "my-tenant")
        assert result == "services__my-tenant"


class TestGraphBuilderCollectionResolution:
    def test_resolve_vector_collection_uses_tenant(self):
        result = resolve_vector_collection("acme")
        assert "acme" in result
        assert result != "services"

    def test_resolve_vector_collection_empty_tenant_returns_base(self):
        result = resolve_vector_collection("")
        assert result == "services"

    def test_resolve_vector_collection_none_tenant_returns_base(self):
        result = resolve_vector_collection(None)
        assert result == "services"
