"""Tests for semantic cache tenant key hardening."""
from orchestrator.app.semantic_cache import CacheConfig, SemanticQueryCache


class TestSemanticCacheTenantKey:
    def test_different_tenants_different_keys(self) -> None:
        cache = SemanticQueryCache(config=CacheConfig(similarity_threshold=0.5))
        embedding = [1.0, 0.0, 0.0]

        cache.store("q1", embedding, {"answer": "a"}, tenant_id="tenant-a", acl_key="acl1")
        cache.store("q1", embedding, {"answer": "b"}, tenant_id="tenant-b", acl_key="acl1")

        assert len(cache._entries) == 2

    def test_same_tenant_same_key_overwrites(self) -> None:
        cache = SemanticQueryCache(config=CacheConfig(similarity_threshold=0.5))
        embedding = [1.0, 0.0, 0.0]

        cache.store("q1", embedding, {"answer": "old"}, tenant_id="t1", acl_key="acl1")
        cache.store("q1", embedding, {"answer": "new"}, tenant_id="t1", acl_key="acl1")

        assert len(cache._entries) == 1
        entry = list(cache._entries.values())[0]
        assert entry.result["answer"] == "new"

    def test_tenant_isolated_lookup(self) -> None:
        cache = SemanticQueryCache(config=CacheConfig(similarity_threshold=0.5))
        embedding = [1.0, 0.0, 0.0]

        cache.store("q1", embedding, {"answer": "a"}, tenant_id="tenant-a", acl_key="acl1")
        cache.store("q1", embedding, {"answer": "b"}, tenant_id="tenant-b", acl_key="acl1")

        result_a = cache.lookup(embedding, tenant_id="tenant-a", acl_key="acl1")
        result_b = cache.lookup(embedding, tenant_id="tenant-b", acl_key="acl1")

        assert result_a is not None
        assert result_b is not None
        assert result_a["answer"] == "a"
        assert result_b["answer"] == "b"
