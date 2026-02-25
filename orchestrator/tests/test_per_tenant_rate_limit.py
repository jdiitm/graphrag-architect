from __future__ import annotations

import asyncio

import pytest

from orchestrator.app.token_bucket import AdaptiveTokenBucket, TenantRateLimiter


class TestAdaptiveTokenBucketTryAcquire:
    @pytest.mark.asyncio
    async def test_try_acquire_succeeds_when_tokens_available(self):
        bucket = AdaptiveTokenBucket(capacity=5, refill_rate=10.0)
        assert await bucket.try_acquire() is True

    @pytest.mark.asyncio
    async def test_try_acquire_fails_when_exhausted(self):
        bucket = AdaptiveTokenBucket(capacity=1, refill_rate=0.001)
        assert await bucket.try_acquire() is True
        assert await bucket.try_acquire() is False


class TestTenantRateLimiter:
    @pytest.mark.asyncio
    async def test_different_tenants_get_independent_buckets(self):
        limiter = TenantRateLimiter(capacity=1, refill_rate=0.001)
        assert await limiter.try_acquire("tenant-a") is True
        assert await limiter.try_acquire("tenant-a") is False
        assert await limiter.try_acquire("tenant-b") is True

    @pytest.mark.asyncio
    async def test_evicts_oldest_when_max_tenants_reached(self):
        limiter = TenantRateLimiter(capacity=5, refill_rate=10.0, max_tenants=2)
        await limiter.try_acquire("t1")
        await limiter.try_acquire("t2")
        await limiter.try_acquire("t3")
        assert limiter.active_tenants == 2

    @pytest.mark.asyncio
    async def test_active_tenants_tracks_count(self):
        limiter = TenantRateLimiter(capacity=5, refill_rate=10.0)
        await limiter.try_acquire("a")
        await limiter.try_acquire("b")
        assert limiter.active_tenants == 2


class TestQueryEndpointRateLimiting:
    @pytest.mark.asyncio
    async def test_query_returns_429_when_rate_limited(self):
        from unittest.mock import patch
        from httpx import ASGITransport, AsyncClient
        from orchestrator.app.main import app, _TENANT_LIMITER

        with patch.object(
            _TENANT_LIMITER, "try_acquire", return_value=False,
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                resp = await client.post(
                    "/query", json={"query": "test", "max_results": 5},
                )
        assert resp.status_code == 429
        assert "rate limit" in resp.json()["detail"].lower()
