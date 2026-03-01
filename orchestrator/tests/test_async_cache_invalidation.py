from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from orchestrator.app.semantic_cache import (
    CacheConfig,
    CacheInvalidationWorker,
    RedisSemanticQueryCache,
)


class TestInvalidateByNodesPublishesToStream:

    @pytest.mark.asyncio
    async def test_publishes_event_to_redis_stream(self) -> None:
        mock_redis = AsyncMock()

        with patch(
            "orchestrator.app.semantic_cache.create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379",
                config=CacheConfig(similarity_threshold=0.9),
            )
            cache._l1.store(
                "q1", [1.0, 0.0], {"a": 1},
                tenant_id="t1", node_ids={"node-A", "node-B"},
            )
            await cache.invalidate_by_nodes({"node-A", "node-B"})

        mock_redis.xadd.assert_called_once()
        call_args = mock_redis.xadd.call_args
        stream_key = call_args[0][0]
        payload = call_args[0][1]
        assert "invalidation:stream" in stream_key
        published_ids = set(json.loads(payload["node_ids"]))
        assert published_ids == {"node-A", "node-B"}

    @pytest.mark.asyncio
    async def test_l1_invalidation_still_synchronous(self) -> None:
        mock_redis = AsyncMock()

        with patch(
            "orchestrator.app.semantic_cache.create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379",
                config=CacheConfig(similarity_threshold=0.9),
            )
            cache._l1.store(
                "q1", [1.0, 0.0], {"a": 1},
                tenant_id="t1", node_ids={"node-A"},
            )
            cache._l1.store(
                "q2", [0.0, 1.0], {"a": 2},
                tenant_id="t1", node_ids={"node-B"},
            )
            removed = await cache.invalidate_by_nodes({"node-A"})

        assert removed == 1
        assert cache._l1.stats().size == 1

    @pytest.mark.asyncio
    async def test_no_lua_script_registered(self) -> None:
        mock_redis = AsyncMock()

        with patch(
            "orchestrator.app.semantic_cache.create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            RedisSemanticQueryCache(
                redis_url="redis://fake:6379",
                config=CacheConfig(similarity_threshold=0.9),
            )

        mock_redis.register_script.assert_not_called()

    @pytest.mark.asyncio
    async def test_stream_failure_is_non_fatal(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.xadd = AsyncMock(
            side_effect=ConnectionError("redis down"),
        )

        with patch(
            "orchestrator.app.semantic_cache.create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379",
                config=CacheConfig(similarity_threshold=0.9),
            )
            cache._l1.store(
                "q1", [1.0, 0.0], {"a": 1},
                tenant_id="t1", node_ids={"node-A"},
            )
            removed = await cache.invalidate_by_nodes({"node-A"})

        assert removed == 1


class TestCacheInvalidationWorkerProcessesBatch:

    @pytest.mark.asyncio
    async def test_processes_single_event(self) -> None:
        mock_redis = AsyncMock()
        event_data = {"node_ids": json.dumps(["node-A", "node-B"])}
        mock_redis.xreadgroup = AsyncMock(return_value=[
            ("stream", [("msg-1", event_data)]),
        ])
        mock_redis.sscan = AsyncMock(return_value=(0, [
            "graphrag:semcache:key1", "graphrag:semcache:key2",
        ]))
        mock_redis.unlink = AsyncMock()
        mock_redis.xack = AsyncMock()

        worker = CacheInvalidationWorker(
            redis_conn=mock_redis,
            key_prefix="graphrag:semcache:",
        )
        invalidated = await worker.process_batch()

        assert invalidated >= 2
        mock_redis.xack.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_stream_returns_zero(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.xreadgroup = AsyncMock(return_value=[])

        worker = CacheInvalidationWorker(
            redis_conn=mock_redis,
            key_prefix="graphrag:semcache:",
        )
        invalidated = await worker.process_batch()

        assert invalidated == 0


class TestWorkerUsesSscanNotSmembers:

    @pytest.mark.asyncio
    async def test_sscan_used_for_tag_iteration(self) -> None:
        mock_redis = AsyncMock()
        event_data = {"node_ids": json.dumps(["node-X"])}
        mock_redis.xreadgroup = AsyncMock(return_value=[
            ("stream", [("msg-1", event_data)]),
        ])
        mock_redis.sscan = AsyncMock(return_value=(0, ["key1"]))
        mock_redis.unlink = AsyncMock()
        mock_redis.xack = AsyncMock()

        worker = CacheInvalidationWorker(
            redis_conn=mock_redis,
            key_prefix="graphrag:semcache:",
        )
        await worker.process_batch()

        mock_redis.sscan.assert_called()
        mock_redis.smembers.assert_not_called()


class TestWorkerUsesUnlinkNotDel:

    @pytest.mark.asyncio
    async def test_unlink_used_for_key_deletion(self) -> None:
        mock_redis = AsyncMock()
        event_data = {"node_ids": json.dumps(["node-Y"])}
        mock_redis.xreadgroup = AsyncMock(return_value=[
            ("stream", [("msg-1", event_data)]),
        ])
        mock_redis.sscan = AsyncMock(return_value=(0, ["key1", "key2"]))
        mock_redis.unlink = AsyncMock()
        mock_redis.xack = AsyncMock()

        worker = CacheInvalidationWorker(
            redis_conn=mock_redis,
            key_prefix="graphrag:semcache:",
        )
        await worker.process_batch()

        mock_redis.unlink.assert_called()
        mock_redis.delete.assert_not_called()


class TestWorkerBoundedBatchSize:

    @pytest.mark.asyncio
    async def test_sscan_count_matches_batch_size(self) -> None:
        mock_redis = AsyncMock()
        event_data = {"node_ids": json.dumps(["node-Z"])}
        mock_redis.xreadgroup = AsyncMock(return_value=[
            ("stream", [("msg-1", event_data)]),
        ])
        mock_redis.sscan = AsyncMock(return_value=(0, []))
        mock_redis.unlink = AsyncMock()
        mock_redis.xack = AsyncMock()

        batch_size = 50
        worker = CacheInvalidationWorker(
            redis_conn=mock_redis,
            key_prefix="graphrag:semcache:",
            batch_size=batch_size,
        )
        await worker.process_batch()

        sscan_call = mock_redis.sscan.call_args
        assert sscan_call.kwargs.get("count") == batch_size or (
            len(sscan_call.args) >= 3 and sscan_call.args[2] == batch_size
        )

    @pytest.mark.asyncio
    async def test_xreadgroup_count_matches_batch_size(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.xreadgroup = AsyncMock(return_value=[])

        batch_size = 75
        worker = CacheInvalidationWorker(
            redis_conn=mock_redis,
            key_prefix="graphrag:semcache:",
            batch_size=batch_size,
        )
        await worker.process_batch()

        xread_call = mock_redis.xreadgroup.call_args
        assert xread_call.kwargs.get("count") == batch_size
