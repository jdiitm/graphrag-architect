from __future__ import annotations

import json
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.ast_dlq import (
    ASTDeadLetterQueue,
    InMemoryASTDLQ,
    RedisASTDLQ,
    create_ast_dlq,
)


class TestInMemoryASTDLQ:

    def test_enqueue_and_peek(self) -> None:
        dlq: ASTDeadLetterQueue = InMemoryASTDLQ(max_size=100)
        payload: Dict[str, Any] = {"raw_files": [{"path": "main.go"}], "tenant_id": "t1"}
        dlq.enqueue(payload)
        items = dlq.peek()
        assert len(items) == 1
        assert items[0]["tenant_id"] == "t1"

    def test_drain_returns_and_removes(self) -> None:
        dlq = InMemoryASTDLQ(max_size=100)
        dlq.enqueue({"index": 0})
        dlq.enqueue({"index": 1})
        drained = dlq.drain()
        assert len(drained) == 2
        assert drained[0]["index"] == 0
        assert dlq.size() == 0

    def test_drain_empty_returns_empty_list(self) -> None:
        dlq = InMemoryASTDLQ(max_size=100)
        assert dlq.drain() == []

    def test_size_tracks_entries(self) -> None:
        dlq = InMemoryASTDLQ(max_size=100)
        assert dlq.size() == 0
        dlq.enqueue({"x": 1})
        assert dlq.size() == 1
        dlq.enqueue({"x": 2})
        assert dlq.size() == 2

    def test_bounded_capacity_evicts_oldest(self) -> None:
        dlq = InMemoryASTDLQ(max_size=3)
        for i in range(5):
            dlq.enqueue({"index": i})
        assert dlq.size() == 3
        items = dlq.peek()
        assert items[0]["index"] == 2
        assert items[2]["index"] == 4

    def test_max_size_must_be_positive(self) -> None:
        with pytest.raises(ValueError):
            InMemoryASTDLQ(max_size=0)
        with pytest.raises(ValueError):
            InMemoryASTDLQ(max_size=-1)

    def test_clear_removes_all(self) -> None:
        dlq = InMemoryASTDLQ(max_size=100)
        dlq.enqueue({"x": 1})
        dlq.enqueue({"x": 2})
        dlq.clear()
        assert dlq.size() == 0
        assert dlq.peek() == []


class TestRedisASTDLQ:

    @pytest.fixture()
    def mock_redis(self) -> MagicMock:
        redis = MagicMock()
        redis.rpush = AsyncMock(return_value=1)
        redis.ltrim = AsyncMock()
        redis.llen = AsyncMock(return_value=0)
        redis.lrange = AsyncMock(return_value=[])
        redis.delete = AsyncMock()
        redis.eval = AsyncMock(return_value=[])
        return redis

    def _make_dlq(
        self, mock_redis: MagicMock, max_size: int = 10_000,
    ) -> RedisASTDLQ:
        with patch(
            "orchestrator.app.ast_dlq.create_async_redis",
            return_value=mock_redis,
        ):
            return RedisASTDLQ(
                redis_url="redis://localhost:6379",
                max_size=max_size,
            )

    @pytest.mark.asyncio
    async def test_enqueue_pushes_to_redis_list(
        self, mock_redis: MagicMock,
    ) -> None:
        dlq = self._make_dlq(mock_redis)
        payload = {"raw_files": [{"path": "a.go"}], "tenant_id": "t1"}
        await dlq.async_enqueue(payload)
        mock_redis.rpush.assert_called_once()
        call_args = mock_redis.rpush.call_args
        assert call_args[0][0] == "graphrag:ast_dlq"
        stored = json.loads(call_args[0][1])
        assert stored["tenant_id"] == "t1"

    @pytest.mark.asyncio
    async def test_enqueue_trims_to_max_size(
        self, mock_redis: MagicMock,
    ) -> None:
        dlq = self._make_dlq(mock_redis, max_size=100)
        await dlq.async_enqueue({"x": 1})
        mock_redis.ltrim.assert_called_once_with(
            "graphrag:ast_dlq", -100, -1,
        )

    @pytest.mark.asyncio
    async def test_drain_returns_and_deletes(
        self, mock_redis: MagicMock,
    ) -> None:
        entries = [
            json.dumps({"index": 0}),
            json.dumps({"index": 1}),
        ]
        mock_redis.eval = AsyncMock(return_value=entries)

        dlq = self._make_dlq(mock_redis)
        result = await dlq.async_drain()
        assert len(result) == 2
        assert result[0]["index"] == 0
        assert result[1]["index"] == 1
        mock_redis.eval.assert_called_once()
        call_args = mock_redis.eval.call_args
        assert call_args[0][1] == 1
        assert call_args[0][2] == "graphrag:ast_dlq"

    @pytest.mark.asyncio
    async def test_drain_empty_list(
        self, mock_redis: MagicMock,
    ) -> None:
        mock_redis.eval = AsyncMock(return_value=[])
        dlq = self._make_dlq(mock_redis)
        result = await dlq.async_drain()
        assert result == []
        mock_redis.eval.assert_called_once()

    @pytest.mark.asyncio
    async def test_size_delegates_to_llen(
        self, mock_redis: MagicMock,
    ) -> None:
        mock_redis.llen = AsyncMock(return_value=42)
        dlq = self._make_dlq(mock_redis)
        result = await dlq.async_size()
        assert result == 42
        mock_redis.llen.assert_called_once_with("graphrag:ast_dlq")

    @pytest.mark.asyncio
    async def test_peek_returns_without_removing(
        self, mock_redis: MagicMock,
    ) -> None:
        entries = [json.dumps({"index": 0})]
        mock_redis.lrange = AsyncMock(return_value=entries)
        dlq = self._make_dlq(mock_redis)
        result = await dlq.async_peek()
        assert len(result) == 1
        assert result[0]["index"] == 0
        mock_redis.delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_clear_deletes_key(
        self, mock_redis: MagicMock,
    ) -> None:
        dlq = self._make_dlq(mock_redis)
        await dlq.async_clear()
        mock_redis.delete.assert_called_once_with("graphrag:ast_dlq")

    @pytest.mark.asyncio
    async def test_custom_key_prefix(
        self, mock_redis: MagicMock,
    ) -> None:
        with patch(
            "orchestrator.app.ast_dlq.create_async_redis",
            return_value=mock_redis,
        ):
            dlq = RedisASTDLQ(
                redis_url="redis://localhost:6379",
                key_prefix="custom:dlq:",
            )
        await dlq.async_enqueue({"x": 1})
        call_args = mock_redis.rpush.call_args
        assert call_args[0][0] == "custom:dlq:ast_dlq"

    def test_sync_enqueue_delegates_to_local(
        self, mock_redis: MagicMock,
    ) -> None:
        dlq = self._make_dlq(mock_redis)
        dlq.enqueue({"tenant_id": "t1"})
        items = dlq.peek()
        assert len(items) == 1
        assert items[0]["tenant_id"] == "t1"

    def test_sync_drain_delegates_to_local(
        self, mock_redis: MagicMock,
    ) -> None:
        dlq = self._make_dlq(mock_redis)
        dlq.enqueue({"index": 0})
        dlq.enqueue({"index": 1})
        result = dlq.drain()
        assert len(result) == 2
        assert dlq.size() == 0

    def test_sync_size_delegates_to_local(
        self, mock_redis: MagicMock,
    ) -> None:
        dlq = self._make_dlq(mock_redis)
        assert dlq.size() == 0
        dlq.enqueue({"x": 1})
        assert dlq.size() == 1


class TestCreateASTDLQ:

    def test_returns_redis_when_configured(self) -> None:
        with patch.dict("os.environ", {
            "REDIS_URL": "redis://localhost:6379",
        }):
            result = create_ast_dlq()
        assert isinstance(result, RedisASTDLQ)

    def test_returns_inmemory_when_no_redis(self) -> None:
        with patch.dict("os.environ", {
            "REDIS_URL": "",
        }, clear=False):
            result = create_ast_dlq()
        assert isinstance(result, InMemoryASTDLQ)

    def test_max_size_from_env(self) -> None:
        with patch.dict("os.environ", {
            "REDIS_URL": "",
            "AST_DLQ_MAX_SIZE": "500",
        }, clear=False):
            result = create_ast_dlq()
        assert isinstance(result, InMemoryASTDLQ)
        assert result._max_size == 500
