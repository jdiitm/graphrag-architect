import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.token_bucket import (
    RedisRateLimiter,
    TenantRateLimiter,
    create_rate_limiter,
)


def _make_pipeline_mock(zcard_result):
    pipe = AsyncMock()
    pipe.zremrangebyscore = MagicMock()
    pipe.zadd = MagicMock()
    pipe.expire = MagicMock()
    pipe.zcard = MagicMock()
    pipe.execute = AsyncMock(return_value=[0, 1, True, zcard_result])

    @asynccontextmanager
    async def _pipeline(transaction=True):
        yield pipe

    return _pipeline, pipe


def _make_redis_mock(zcard_result):
    r = AsyncMock()
    pipeline_fn, pipe = _make_pipeline_mock(zcard_result)
    r.pipeline = pipeline_fn
    r.zrem = AsyncMock()
    return r, pipe


@pytest.fixture
def redis_limiter_allow():
    mock_redis, _ = _make_redis_mock(zcard_result=3)
    with patch(
        "orchestrator.app.token_bucket.create_async_redis",
        return_value=mock_redis,
    ):
        limiter = RedisRateLimiter(
            redis_url="redis://localhost:6379", capacity=5, window_seconds=60,
        )
    limiter._redis = mock_redis
    return limiter


@pytest.fixture
def redis_limiter_deny():
    mock_redis, _ = _make_redis_mock(zcard_result=6)
    with patch(
        "orchestrator.app.token_bucket.create_async_redis",
        return_value=mock_redis,
    ):
        limiter = RedisRateLimiter(
            redis_url="redis://localhost:6379", capacity=5, window_seconds=60,
        )
    limiter._redis = mock_redis
    return limiter


def test_try_acquire_succeeds_within_capacity(redis_limiter_allow):
    loop = asyncio.new_event_loop()

    async def _run():
        result = await redis_limiter_allow.try_acquire("acme")
        assert result is True

    loop.run_until_complete(_run())
    loop.close()


def test_try_acquire_rejects_over_capacity(redis_limiter_deny):
    loop = asyncio.new_event_loop()

    async def _run():
        result = await redis_limiter_deny.try_acquire("acme")
        assert result is False

    loop.run_until_complete(_run())
    loop.close()


def test_factory_returns_redis_when_url_set():
    with patch.dict("os.environ", {"REDIS_URL": "redis://localhost:6379"}):
        limiter = create_rate_limiter()
    assert isinstance(limiter, RedisRateLimiter)


def test_factory_returns_local_when_no_url():
    with patch.dict("os.environ", {"REDIS_URL": ""}, clear=False):
        limiter = create_rate_limiter()
    assert isinstance(limiter, TenantRateLimiter)
    assert not isinstance(limiter, RedisRateLimiter)
