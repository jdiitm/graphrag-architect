from orchestrator.app.graph_builder import resolve_vector_collection


class TestGraphBuilderCollectionResolution:
    def test_resolve_vector_collection_ignores_tenant(self):
        result = resolve_vector_collection("acme")
        assert result == "service_embeddings"

    def test_resolve_vector_collection_empty_tenant_returns_base(self):
        result = resolve_vector_collection("")
        assert result == "service_embeddings"

    def test_resolve_vector_collection_none_tenant_returns_base(self):
        result = resolve_vector_collection(None)
        assert result == "service_embeddings"

    def test_all_tenants_share_single_collection(self):
        assert resolve_vector_collection("acme") == resolve_vector_collection("globex")
