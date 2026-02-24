from __future__ import annotations

from unittest.mock import AsyncMock, patch

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


class TestSubgraphCacheWiredIntoQueryEngine:

    @pytest.mark.asyncio
    async def test_cached_result_skips_neo4j_execution(self) -> None:
        from unittest.mock import AsyncMock, MagicMock, patch
        from orchestrator.app.query_engine import _execute_sandboxed_read

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(return_value=[{"x": 1}])
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        cypher = "MATCH (n:Service) RETURN n"
        params: dict[str, str] = {}

        with patch(
            "orchestrator.app.query_engine._sandbox_explain_check",
            new_callable=AsyncMock,
        ), patch(
            "orchestrator.app.query_engine._get_query_timeout",
            return_value=30.0,
        ):
            result1 = await _execute_sandboxed_read(mock_driver, cypher, params)
            result2 = await _execute_sandboxed_read(mock_driver, cypher, params)

        assert result1 == [{"x": 1}]
        assert result2 == [{"x": 1}]
        assert mock_session.execute_read.await_count == 1, (
            "Second call should be served from cache, not Neo4j"
        )


class TestNormalizeCypher:
    def test_normalize_cypher_produces_consistent_keys(self) -> None:
        q1 = "MATCH (n:Service) RETURN n"
        q2 = "match (n:Service) return n"
        assert normalize_cypher(q1) == normalize_cypher(q2)

    def test_normalize_cypher_handles_whitespace_variations(self) -> None:
        q1 = "  MATCH   (n)   RETURN n  ;  "
        q2 = "match (n) return n"
        assert normalize_cypher(q1) == normalize_cypher(q2)


class TestCacheKey:
    def test_same_query_same_acl_same_key(self) -> None:
        from orchestrator.app.subgraph_cache import cache_key
        k1 = cache_key("MATCH (n) RETURN n", {"acl_team": "ops"})
        k2 = cache_key("MATCH (n) RETURN n", {"acl_team": "ops"})
        assert k1 == k2

    def test_different_acl_different_key(self) -> None:
        from orchestrator.app.subgraph_cache import cache_key
        k1 = cache_key("MATCH (n) RETURN n", {"acl_team": "ops"})
        k2 = cache_key("MATCH (n) RETURN n", {"acl_team": "dev"})
        assert k1 != k2


class TestRedisSubgraphCache:
    @pytest.mark.asyncio
    async def test_l1_hit_avoids_redis(self) -> None:
        from orchestrator.app.subgraph_cache import RedisSubgraphCache
        mock_redis = AsyncMock()
        with patch("redis.asyncio.from_url", return_value=mock_redis):
            cache = RedisSubgraphCache(redis_url="redis://localhost:6379")
        cache._l1.put("k1", [{"a": 1}])
        result = await cache.get("k1")
        assert result == [{"a": 1}]
        mock_redis.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_l2_hit_populates_l1(self) -> None:
        from orchestrator.app.subgraph_cache import RedisSubgraphCache
        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value='[{"b": 2}]')
        with patch("redis.asyncio.from_url", return_value=mock_redis):
            cache = RedisSubgraphCache(redis_url="redis://localhost:6379")
        result = await cache.get("k2")
        assert result == [{"b": 2}]
        l1_result = cache._l1.get("k2")
        assert l1_result == [{"b": 2}]

    @pytest.mark.asyncio
    async def test_put_writes_to_both_tiers(self) -> None:
        from orchestrator.app.subgraph_cache import RedisSubgraphCache
        mock_redis = AsyncMock()
        with patch("redis.asyncio.from_url", return_value=mock_redis):
            cache = RedisSubgraphCache(redis_url="redis://localhost:6379", ttl_seconds=60)
        await cache.put("k3", [{"c": 3}])
        assert cache._l1.get("k3") == [{"c": 3}]
        mock_redis.setex.assert_awaited_once()
