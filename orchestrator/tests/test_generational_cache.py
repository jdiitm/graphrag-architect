from orchestrator.app.subgraph_cache import SubgraphCache
from orchestrator.app.semantic_cache import SemanticQueryCache


class TestSubgraphCacheGenerational:
    def test_advance_generation_increments(self):
        cache = SubgraphCache(maxsize=256)
        gen0 = cache.generation
        cache.advance_generation()
        assert cache.generation == gen0 + 1

    def test_entries_survive_same_generation(self):
        cache = SubgraphCache(maxsize=256)
        cache.put("key1", [{"name": "svc-a"}])
        cache.invalidate_stale()
        assert cache.get("key1") is not None

    def test_old_entries_evicted_after_advance(self):
        cache = SubgraphCache(maxsize=256)
        cache.put("key1", [{"name": "svc-a"}])
        cache.advance_generation()
        cache.invalidate_stale()
        assert cache.get("key1") is None

    def test_new_entries_survive_after_advance(self):
        cache = SubgraphCache(maxsize=256)
        cache.put("key1", [{"name": "svc-a"}])
        cache.advance_generation()
        cache.put("key2", [{"name": "svc-b"}])
        cache.invalidate_stale()
        assert cache.get("key1") is None
        assert cache.get("key2") is not None

    def test_invalidate_all_still_works(self):
        cache = SubgraphCache(maxsize=256)
        cache.put("key1", [{"name": "svc-a"}])
        cache.invalidate_all()
        assert cache.get("key1") is None


class TestSemanticCacheGenerational:
    def _emb(self, seed: float) -> list:
        return [seed * 0.1] * 32

    def test_advance_generation_increments(self):
        cache = SemanticQueryCache()
        gen0 = cache.generation
        cache.advance_generation()
        assert cache.generation == gen0 + 1

    def test_entries_survive_same_generation(self):
        cache = SemanticQueryCache()
        emb = self._emb(1.0)
        cache.store(query="q", query_embedding=emb, result={"a": "b"}, tenant_id="t1")
        cache.invalidate_stale()
        assert cache.lookup(emb, tenant_id="t1") is not None

    def test_old_entries_evicted_after_advance(self):
        cache = SemanticQueryCache()
        emb = self._emb(2.0)
        cache.store(query="q", query_embedding=emb, result={"a": "b"}, tenant_id="t1")
        cache.advance_generation()
        cache.invalidate_stale()
        assert cache.lookup(emb, tenant_id="t1") is None

    def test_new_entries_survive_after_advance(self):
        cache = SemanticQueryCache()
        emb_old = [float(i) for i in range(32)]
        cache.store(query="q", query_embedding=emb_old, result={"a": "b"}, tenant_id="t1")
        cache.advance_generation()
        emb_new = [float(31 - i) for i in range(32)]
        cache.store(query="q2", query_embedding=emb_new, result={"c": "d"}, tenant_id="t1")
        cache.invalidate_stale()
        assert cache.lookup(emb_old, tenant_id="t1") is None
        assert cache.lookup(emb_new, tenant_id="t1") is not None

    def test_invalidate_tenant_still_works(self):
        cache = SemanticQueryCache()
        emb = self._emb(5.0)
        cache.store(query="q", query_embedding=emb, result={"a": "b"}, tenant_id="t1")
        count = cache.invalidate_tenant("t1")
        assert count == 1
        assert cache.lookup(emb, tenant_id="t1") is None
