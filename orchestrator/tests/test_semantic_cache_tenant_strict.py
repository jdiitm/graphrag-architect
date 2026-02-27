from __future__ import annotations

import pytest

from orchestrator.app.semantic_cache import CacheConfig, SemanticQueryCache


class TestStrictTenantIsolation:

    def _make_cache(self) -> SemanticQueryCache:
        return SemanticQueryCache(
            CacheConfig(similarity_threshold=0.9),
        )

    def test_tenant_scoped_entry_invisible_to_empty_tenant_lookup(
        self,
    ) -> None:
        cache = self._make_cache()
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "secret"}, tenant_id="t1")
        result = cache.lookup(embedding, tenant_id="")
        assert result is None, (
            "Tenant-scoped entry must NOT be visible to unscoped lookup"
        )

    def test_empty_tenant_entry_invisible_to_scoped_lookup(self) -> None:
        cache = self._make_cache()
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "global"}, tenant_id="")
        result = cache.lookup(embedding, tenant_id="t1")
        assert result is None, (
            "Unscoped entry must NOT be visible to tenant-scoped lookup"
        )

    def test_cross_tenant_never_leaks(self) -> None:
        cache = self._make_cache()
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "t1-data"}, tenant_id="t1")
        assert cache.lookup(embedding, tenant_id="t2") is None
        assert cache.lookup(embedding, tenant_id="t1") == {"answer": "t1-data"}

    def test_same_tenant_sees_own_entries(self) -> None:
        cache = self._make_cache()
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "mine"}, tenant_id="acme")
        assert cache.lookup(embedding, tenant_id="acme") == {"answer": "mine"}

    def test_unscoped_lookup_sees_only_unscoped_entries(self) -> None:
        cache = self._make_cache()
        emb1 = [1.0, 0.0, 0.0]
        emb2 = [0.0, 1.0, 0.0]
        cache.store("q1", emb1, {"answer": "global"}, tenant_id="")
        cache.store("q2", emb2, {"answer": "tenant"}, tenant_id="t1")
        assert cache.lookup(emb1, tenant_id="") == {"answer": "global"}
        assert cache.lookup(emb2, tenant_id="") is None
