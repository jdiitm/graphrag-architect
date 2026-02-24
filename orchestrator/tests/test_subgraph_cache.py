from __future__ import annotations

import pytest

from orchestrator.app.subgraph_cache import (
    CacheStats,
    SubgraphCache,
    normalize_cypher,
)


class TestSubgraphCacheGet:
    def test_get_on_empty_cache_returns_none(self) -> None:
        cache = SubgraphCache(maxsize=256)
        assert cache.get("any_key") is None


class TestSubgraphCachePutGet:
    def test_put_then_get_returns_value(self) -> None:
        cache = SubgraphCache(maxsize=256)
        value: list[dict[str, object]] = [{"node": "a"}, {"rel": "DEPENDS_ON"}]
        cache.put("query_key", value)
        assert cache.get("query_key") == value


class TestSubgraphCacheEviction:
    def test_cache_evicts_oldest_entry_when_full(self) -> None:
        cache = SubgraphCache(maxsize=3)
        cache.put("a", [{"a": 1}])
        cache.put("b", [{"b": 2}])
        cache.put("c", [{"c": 3}])
        cache.get("a")
        cache.put("d", [{"d": 4}])
        assert cache.get("a") == [{"a": 1}]
        assert cache.get("b") is None
        assert cache.get("c") == [{"c": 3}]
        assert cache.get("d") == [{"d": 4}]


class TestSubgraphCacheInvalidate:
    def test_invalidate_removes_specific_entry(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("key1", [{"x": 1}])
        cache.put("key2", [{"y": 2}])
        cache.invalidate("key1")
        assert cache.get("key1") is None
        assert cache.get("key2") == [{"y": 2}]

    def test_invalidate_all_clears_cache(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("key1", [{"x": 1}])
        cache.put("key2", [{"y": 2}])
        cache.invalidate_all()
        assert cache.get("key1") is None
        assert cache.get("key2") is None


class TestSubgraphCacheStats:
    def test_stats_reports_hits_and_misses_correctly(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("k", [{"v": 1}])
        cache.get("k")
        cache.get("k")
        cache.get("missing")
        stats = cache.stats()
        assert stats.hits == 2
        assert stats.misses == 1
        assert stats.size == 1
        assert stats.maxsize == 256


class TestNormalizeCypher:
    def test_normalize_cypher_produces_consistent_keys(self) -> None:
        q1 = "MATCH (n:Service) RETURN n"
        q2 = "match (n:Service) return n"
        assert normalize_cypher(q1) == normalize_cypher(q2)

    def test_normalize_cypher_handles_whitespace_variations(self) -> None:
        q1 = "  MATCH   (n)   RETURN n  ;  "
        q2 = "match (n) return n"
        assert normalize_cypher(q1) == normalize_cypher(q2)
