import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.query_models import (
    JobStatus,
    QueryJobStore,
    QueryResponse,
    RedisQueryJobStore,
    create_job_store,
)


@pytest.fixture
def mock_redis():
    r = AsyncMock()
    r.get = AsyncMock(return_value=None)
    r.setex = AsyncMock()
    r.delete = AsyncMock()
    return r


@pytest.fixture
def redis_store(mock_redis):
    with patch(
        "orchestrator.app.query_models.create_async_redis", return_value=mock_redis,
    ):
        store = RedisQueryJobStore(redis_url="redis://localhost:6379")
    store._redis = mock_redis
    return store


def test_create_stores_and_retrieves(redis_store, mock_redis):
    loop = asyncio.new_event_loop()

    async def _run():
        job = await redis_store.create()
        assert job.status == JobStatus.PENDING
        assert job.job_id

        mock_redis.get.return_value = job.model_dump_json()
        retrieved = await redis_store.get(job.job_id)
        assert retrieved is not None
        assert retrieved.job_id == job.job_id

    loop.run_until_complete(_run())
    loop.close()


def test_mark_running(redis_store, mock_redis):
    loop = asyncio.new_event_loop()

    async def _run():
        job = await redis_store.create()
        mock_redis.get.return_value = job.model_dump_json()
        await redis_store.mark_running(job.job_id)
        assert mock_redis.setex.call_count >= 2

    loop.run_until_complete(_run())
    loop.close()


def test_complete_stores_result(redis_store, mock_redis):
    loop = asyncio.new_event_loop()

    async def _run():
        job = await redis_store.create()
        result = QueryResponse(
            answer="test", sources=[], complexity="entity_lookup",
            retrieval_path="vector",
        )
        mock_redis.get.return_value = job.model_dump_json()
        await redis_store.complete(job.job_id, result)
        call_args = mock_redis.setex.call_args
        stored = json.loads(call_args[0][2])
        assert stored["status"] == "completed"

    loop.run_until_complete(_run())
    loop.close()


def test_fail_stores_error(redis_store, mock_redis):
    loop = asyncio.new_event_loop()

    async def _run():
        job = await redis_store.create()
        mock_redis.get.return_value = job.model_dump_json()
        await redis_store.fail(job.job_id, "boom")
        call_args = mock_redis.setex.call_args
        stored = json.loads(call_args[0][2])
        assert stored["status"] == "failed"
        assert stored["error"] == "boom"

    loop.run_until_complete(_run())
    loop.close()


def test_get_returns_none_for_unknown(redis_store, mock_redis):
    loop = asyncio.new_event_loop()

    async def _run():
        mock_redis.get.return_value = None
        result = await redis_store.get("nonexistent")
        assert result is None

    loop.run_until_complete(_run())
    loop.close()


def test_factory_returns_redis_when_url_set():
    with patch.dict("os.environ", {"REDIS_URL": "redis://localhost:6379"}):
        store = create_job_store()
    assert isinstance(store, RedisQueryJobStore)


def test_factory_returns_local_when_no_url():
    with patch.dict("os.environ", {"REDIS_URL": ""}, clear=False):
        store = create_job_store()
    assert isinstance(store, QueryJobStore)
    assert not isinstance(store, RedisQueryJobStore)
