import asyncio
import hashlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
)
from orchestrator.app.distributed_lock import BoundedTaskSet
from orchestrator.app.subgraph_cache import SubgraphCache, cache_key


class TestCircuitBreakerJitter:

    def test_config_has_jitter_factor_field(self) -> None:
        cfg = CircuitBreakerConfig(jitter_factor=0.25)
        assert cfg.jitter_factor == 0.25

    def test_config_jitter_factor_defaults_to_nonzero(self) -> None:
        cfg = CircuitBreakerConfig()
        assert cfg.jitter_factor > 0.0, (
            "Default jitter_factor must be > 0 to prevent thundering-herd "
            "on circuit breaker recovery"
        )

    @pytest.mark.asyncio
    async def test_recovery_timeout_varies_with_jitter(self) -> None:
        cfg = CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout=1.0,
            jitter_factor=0.5,
        )
        observed_transitions: list[float] = []
        for _ in range(10):
            cb = CircuitBreaker(config=cfg)
            failing = AsyncMock(side_effect=ConnectionError("down"))
            with pytest.raises(ConnectionError):
                await cb.call(failing)
            assert cb.state == CircuitState.OPEN

            start = asyncio.get_event_loop().time()
            while cb.state == CircuitState.OPEN:
                await asyncio.sleep(0.05)
                if asyncio.get_event_loop().time() - start > 3.0:
                    break
            transition_time = asyncio.get_event_loop().time() - start
            observed_transitions.append(transition_time)

        assert len(set(round(t, 2) for t in observed_transitions)) > 1, (
            "With jitter_factor=0.5, recovery transitions should occur at "
            "varying times, not all identically. Observed: "
            f"{observed_transitions}"
        )


class TestBoundedTaskSetDrain:

    @pytest.mark.asyncio
    async def test_drain_all_exists(self) -> None:
        bts = BoundedTaskSet(max_tasks=10)
        assert hasattr(bts, "drain_all"), (
            "BoundedTaskSet must expose drain_all for graceful shutdown"
        )

    @pytest.mark.asyncio
    async def test_drain_all_waits_for_pending_tasks(self) -> None:
        bts = BoundedTaskSet(max_tasks=10)
        completed = []

        async def slow_task() -> None:
            await asyncio.sleep(0.1)
            completed.append(True)

        task = asyncio.create_task(slow_task())
        bts.try_add(task)
        assert bts.active_count == 1

        drained = await bts.drain_all(timeout=5.0)
        assert drained == 1
        assert len(completed) == 1, (
            "drain_all must wait for all pending tasks to complete"
        )

    @pytest.mark.asyncio
    async def test_drain_all_returns_zero_when_empty(self) -> None:
        bts = BoundedTaskSet(max_tasks=10)
        drained = await bts.drain_all(timeout=1.0)
        assert drained == 0

    @pytest.mark.asyncio
    async def test_drain_all_respects_timeout(self) -> None:
        bts = BoundedTaskSet(max_tasks=10)

        async def blocking_task() -> None:
            await asyncio.sleep(60.0)

        task = asyncio.create_task(blocking_task())
        bts.try_add(task)

        drained = await bts.drain_all(timeout=0.2)
        assert drained == 0, (
            "drain_all must return 0 when tasks don't complete within timeout"
        )
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


class TestCacheKeyUsedInQueryEngine:

    @pytest.mark.asyncio
    async def test_cache_keys_are_fixed_length_hashes(self) -> None:
        from orchestrator.app.query_engine import _execute_sandboxed_read

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(return_value=[{"id": "svc-a"}])
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        captured_keys: list[str] = []
        original_cache_get = None

        from orchestrator.app.query_engine import _SUBGRAPH_CACHE

        original_get = _SUBGRAPH_CACHE.get
        original_put = _SUBGRAPH_CACHE.put if hasattr(_SUBGRAPH_CACHE, "put") else None

        def capture_get(key: str):
            captured_keys.append(key)
            return original_get(key) if callable(original_get) else None

        with patch.object(
            _SUBGRAPH_CACHE, "get", side_effect=capture_get,
        ), patch(
            "orchestrator.app.query_engine._get_query_timeout",
            return_value=30.0,
        ):
            cypher = "MATCH (n:Service) RETURN n"
            long_acl = {
                "acl_team": "infrastructure-platform-engineering",
                "acl_namespace": "production-us-east-1",
                "tenant_id": "tenant-abc-123-very-long-identifier",
            }
            await _execute_sandboxed_read(mock_driver, cypher, long_acl)

        assert len(captured_keys) >= 1
        key = captured_keys[0]
        assert len(key) <= 128, (
            f"Cache key must be a fixed-length hash, not a raw string. "
            f"Got key of length {len(key)}: {key[:80]}..."
        )


class TestSubgraphCacheFactoryByteBudget:

    def test_create_subgraph_cache_has_byte_limit(self) -> None:
        import os
        os.environ.pop("REDIS_URL", None)
        from orchestrator.app.subgraph_cache import create_subgraph_cache
        cache = create_subgraph_cache()
        assert isinstance(cache, SubgraphCache)
        assert cache._max_value_bytes > 0, (
            "Factory-created SubgraphCache must have a non-zero "
            "max_value_bytes to prevent OOM from oversized query results"
        )


class TestRedisSubgraphCacheNodeInvalidation:

    @pytest.mark.asyncio
    async def test_put_stores_node_tags_in_redis(self) -> None:
        from orchestrator.app.subgraph_cache import RedisSubgraphCache
        mock_redis = AsyncMock()
        mock_redis.setex = AsyncMock()
        mock_redis.sadd = AsyncMock()
        mock_redis.expire = AsyncMock()
        with patch("redis.asyncio.from_url", return_value=mock_redis):
            cache = RedisSubgraphCache(redis_url="redis://localhost:6379")
        await cache.put(
            "query1",
            [{"id": "svc-a"}],
            node_ids={"node-1", "node-2"},
        )
        assert mock_redis.sadd.call_count >= 2, (
            "put() with node_ids must store reverse-index tags in Redis "
            "for surgical invalidation"
        )

    @pytest.mark.asyncio
    async def test_invalidate_by_nodes_cleans_redis(self) -> None:
        from orchestrator.app.subgraph_cache import RedisSubgraphCache
        mock_redis = AsyncMock()
        mock_redis.smembers = AsyncMock(return_value={b"graphrag:sgcache:query1"})
        mock_redis.delete = AsyncMock()
        with patch("redis.asyncio.from_url", return_value=mock_redis):
            cache = RedisSubgraphCache(redis_url="redis://localhost:6379")
        cache._l1.put("query1", [{"a": 1}], node_ids={"node-1"})
        result = await cache.invalidate_by_nodes({"node-1"})
        assert result >= 1
        assert mock_redis.smembers.called, (
            "invalidate_by_nodes must query Redis for node tags"
        )
        assert mock_redis.delete.called, (
            "invalidate_by_nodes must delete matched keys from Redis"
        )
