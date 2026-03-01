from unittest.mock import AsyncMock

import pytest

from orchestrator.app.neo4j_pool import (
    RedisTenantConnectionTracker,
    TenantConnectionTracker,
    TenantQuotaExceededError,
    create_connection_tracker,
)


class TestRedisTenantConnectionTracker:

    @pytest.mark.asyncio
    async def test_acquire_increments_counter(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.eval.return_value = 1
        tracker = RedisTenantConnectionTracker(
            redis_conn=mock_redis, pool_size=100, max_tenant_fraction=0.2,
        )
        await tracker.acquire("tenant-1")
        mock_redis.eval.assert_called_once()

    @pytest.mark.asyncio
    async def test_release_decrements_counter(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.eval.return_value = 0
        tracker = RedisTenantConnectionTracker(
            redis_conn=mock_redis, pool_size=100, max_tenant_fraction=0.2,
        )
        await tracker.release("tenant-1")
        mock_redis.eval.assert_called_once()

    @pytest.mark.asyncio
    async def test_acquire_exceeds_quota_raises(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.eval.return_value = 0
        tracker = RedisTenantConnectionTracker(
            redis_conn=mock_redis, pool_size=100, max_tenant_fraction=0.2,
        )
        with pytest.raises(TenantQuotaExceededError):
            await tracker.acquire("tenant-1")

    @pytest.mark.asyncio
    async def test_max_per_tenant_property(self) -> None:
        mock_redis = AsyncMock()
        tracker = RedisTenantConnectionTracker(
            redis_conn=mock_redis, pool_size=100, max_tenant_fraction=0.3,
        )
        assert tracker.max_per_tenant == 30

    @pytest.mark.asyncio
    async def test_active_count_returns_cached_value(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.get.return_value = "5"
        tracker = RedisTenantConnectionTracker(
            redis_conn=mock_redis, pool_size=100, max_tenant_fraction=0.2,
        )
        count = await tracker.active_count("tenant-1")
        assert count == 5

    @pytest.mark.asyncio
    async def test_active_count_returns_zero_when_absent(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.get.return_value = None
        tracker = RedisTenantConnectionTracker(
            redis_conn=mock_redis, pool_size=100, max_tenant_fraction=0.2,
        )
        count = await tracker.active_count("tenant-1")
        assert count == 0


class TestCreateConnectionTracker:

    def test_returns_redis_tracker_when_redis_provided(self) -> None:
        mock_redis = AsyncMock()
        tracker = create_connection_tracker(
            pool_size=100, redis_conn=mock_redis,
        )
        assert isinstance(tracker, RedisTenantConnectionTracker)

    def test_returns_local_tracker_when_no_redis(self) -> None:
        tracker = create_connection_tracker(pool_size=100)
        assert isinstance(tracker, TenantConnectionTracker)
        assert not isinstance(tracker, RedisTenantConnectionTracker)

    def test_local_tracker_preserves_max_fraction(self) -> None:
        tracker = create_connection_tracker(
            pool_size=50, max_tenant_fraction=0.4,
        )
        assert isinstance(tracker, TenantConnectionTracker)
        assert tracker.max_per_tenant == 20
