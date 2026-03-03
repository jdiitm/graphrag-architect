"""Tests for RedisQueryJobStore atomic state transitions."""
import time
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.query_models import (
    JobStatus,
    QueryJobResponse,
    QueryResponse,
    RedisQueryJobStore,
)


def _make_job_json(status: str = "pending", job_id: str = "job-1") -> str:
    job = QueryJobResponse(
        job_id=job_id, status=JobStatus(status),
        tenant_id="t1", created_at=time.time(),
    )
    return job.model_dump_json()


@pytest.fixture
def redis_store() -> RedisQueryJobStore:
    with patch("orchestrator.app.query_models.require_redis"):
        with patch("orchestrator.app.query_models.create_async_redis") as mock_redis:
            mock_conn = AsyncMock()
            mock_redis.return_value = mock_conn
            store = RedisQueryJobStore(
                redis_url="redis://localhost", ttl_seconds=300,
            )
    return store


class TestRedisJobStoreAtomicTransitions:
    @pytest.mark.asyncio
    async def test_mark_running_uses_lua_with_pending_guard(
        self, redis_store: RedisQueryJobStore,
    ) -> None:
        redis_store._redis.get = AsyncMock(
            return_value=_make_job_json("pending"),
        )
        redis_store._redis.eval = AsyncMock(return_value=1)
        await redis_store.mark_running("job-1")
        redis_store._redis.eval.assert_called_once()
        args = redis_store._redis.eval.call_args[0]
        assert args[3] == "pending"

    @pytest.mark.asyncio
    async def test_complete_uses_lua_with_running_guard(
        self, redis_store: RedisQueryJobStore,
    ) -> None:
        redis_store._redis.get = AsyncMock(
            return_value=_make_job_json("running"),
        )
        redis_store._redis.eval = AsyncMock(return_value=1)
        result = QueryResponse(
            answer="test", sources=[], complexity="entity_lookup",
            retrieval_path="vector",
        )
        await redis_store.complete("job-1", result)
        redis_store._redis.eval.assert_called_once()
        args = redis_store._redis.eval.call_args[0]
        assert args[3] == "running"

    @pytest.mark.asyncio
    async def test_fail_uses_lua_with_running_guard(
        self, redis_store: RedisQueryJobStore,
    ) -> None:
        redis_store._redis.get = AsyncMock(
            return_value=_make_job_json("running"),
        )
        redis_store._redis.eval = AsyncMock(return_value=1)
        await redis_store.fail("job-1", "timeout")
        redis_store._redis.eval.assert_called_once()
        args = redis_store._redis.eval.call_args[0]
        assert args[3] == "running"

    @pytest.mark.asyncio
    async def test_mark_running_noop_when_job_missing(
        self, redis_store: RedisQueryJobStore,
    ) -> None:
        redis_store._redis.get = AsyncMock(return_value=None)
        redis_store._redis.eval = AsyncMock()
        await redis_store.mark_running("nonexistent")
        redis_store._redis.eval.assert_not_called()

    @pytest.mark.asyncio
    async def test_lua_script_checks_expected_status(
        self, redis_store: RedisQueryJobStore,
    ) -> None:
        redis_store._redis.get = AsyncMock(
            return_value=_make_job_json("pending"),
        )
        redis_store._redis.eval = AsyncMock(return_value=0)
        await redis_store.mark_running("job-1")
        redis_store._redis.eval.assert_called_once()
        lua_script = redis_store._redis.eval.call_args[0][0]
        assert "expected_status" in lua_script
        assert "SETEX" in lua_script
