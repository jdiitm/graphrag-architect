from __future__ import annotations

import os
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.token_bucket import RedisRateLimiter


def _build_mock_redis(
    pipeline_execute_side_effect=None,
    pipeline_execute_return=None,
):
    mock_redis = MagicMock()
    mock_pipeline = MagicMock()
    mock_pipeline.zremrangebyscore = MagicMock()
    mock_pipeline.zadd = MagicMock()
    mock_pipeline.expire = MagicMock()
    mock_pipeline.zcard = MagicMock()

    if pipeline_execute_side_effect:
        mock_pipeline.execute = AsyncMock(side_effect=pipeline_execute_side_effect)
    else:
        mock_pipeline.execute = AsyncMock(
            return_value=pipeline_execute_return or [None, None, None, 5],
        )

    @asynccontextmanager
    async def _pipeline_ctx(**_kwargs):
        yield mock_pipeline

    mock_redis.pipeline = _pipeline_ctx
    mock_redis.zrem = AsyncMock()
    return mock_redis


class TestRedisRateLimiterFailClosed:
    @pytest.mark.asyncio
    async def test_returns_false_on_redis_failure(self) -> None:
        mock_redis = _build_mock_redis(
            pipeline_execute_side_effect=ConnectionError("Redis down"),
        )
        with patch(
            "orchestrator.app.token_bucket.create_async_redis",
            return_value=mock_redis,
        ), patch("orchestrator.app.token_bucket.require_redis"):
            limiter = RedisRateLimiter(redis_url="redis://fake:6379")
            result = await limiter.try_acquire("tenant-1")
            assert result is False, (
                "Rate limiter must fail-closed (deny) when Redis is unavailable"
            )

    @pytest.mark.asyncio
    async def test_returns_true_on_success(self) -> None:
        mock_redis = _build_mock_redis(
            pipeline_execute_return=[None, None, None, 5],
        )
        with patch(
            "orchestrator.app.token_bucket.create_async_redis",
            return_value=mock_redis,
        ), patch("orchestrator.app.token_bucket.require_redis"):
            limiter = RedisRateLimiter(redis_url="redis://fake:6379")
            result = await limiter.try_acquire("tenant-1")
            assert result is True

    @pytest.mark.asyncio
    async def test_fail_open_opt_in(self) -> None:
        mock_redis = _build_mock_redis(
            pipeline_execute_side_effect=ConnectionError("Redis down"),
        )
        with patch.dict(os.environ, {"RATE_LIMIT_FAIL_STRATEGY": "open"}), patch(
            "orchestrator.app.token_bucket.create_async_redis",
            return_value=mock_redis,
        ), patch("orchestrator.app.token_bucket.require_redis"):
            limiter = RedisRateLimiter(
                redis_url="redis://fake:6379",
                fail_strategy="open",
            )
            result = await limiter.try_acquire("tenant-1")
            assert result is True, (
                "Rate limiter must allow requests when explicitly configured fail-open"
            )
