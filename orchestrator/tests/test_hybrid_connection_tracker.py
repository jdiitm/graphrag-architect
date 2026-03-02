from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.neo4j_pool import (
    HybridConnectionTracker,
    TenantConnectionTracker,
    TenantQuotaExceededError,
    create_connection_tracker,
)


class TestHybridConnectionTracker:
    @pytest.fixture
    def mock_redis(self) -> AsyncMock:
        redis = AsyncMock()
        redis.set = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        redis.eval = AsyncMock(return_value=1)
        return redis

    @pytest.mark.asyncio
    async def test_fast_path_acquire_no_redis_call(
        self, mock_redis: AsyncMock,
    ) -> None:
        tracker = HybridConnectionTracker(
            pool_size=100,
            max_tenant_fraction=0.2,
            redis_conn=mock_redis,
        )
        await tracker.acquire("tenant-1")
        mock_redis.eval.assert_not_called()

    @pytest.mark.asyncio
    async def test_fast_path_release_no_redis_call(
        self, mock_redis: AsyncMock,
    ) -> None:
        tracker = HybridConnectionTracker(
            pool_size=100,
            max_tenant_fraction=0.2,
            redis_conn=mock_redis,
        )
        await tracker.acquire("tenant-1")
        await tracker.release("tenant-1")
        mock_redis.eval.assert_not_called()

    @pytest.mark.asyncio
    async def test_local_quota_enforced(
        self, mock_redis: AsyncMock,
    ) -> None:
        tracker = HybridConnectionTracker(
            pool_size=10,
            max_tenant_fraction=0.2,
            redis_conn=mock_redis,
        )
        await tracker.acquire("tenant-1")
        await tracker.acquire("tenant-1")
        with pytest.raises(TenantQuotaExceededError):
            await tracker.acquire("tenant-1")

    @pytest.mark.asyncio
    async def test_active_count_reflects_local_state(
        self, mock_redis: AsyncMock,
    ) -> None:
        tracker = HybridConnectionTracker(
            pool_size=100,
            max_tenant_fraction=0.2,
            redis_conn=mock_redis,
        )
        assert await tracker.active_count("tenant-1") == 0
        await tracker.acquire("tenant-1")
        assert await tracker.active_count("tenant-1") == 1
        await tracker.release("tenant-1")
        assert await tracker.active_count("tenant-1") == 0

    @pytest.mark.asyncio
    async def test_max_per_tenant_property(
        self, mock_redis: AsyncMock,
    ) -> None:
        tracker = HybridConnectionTracker(
            pool_size=100,
            max_tenant_fraction=0.5,
            redis_conn=mock_redis,
        )
        assert tracker.max_per_tenant == 50

    @pytest.mark.asyncio
    async def test_sync_reports_to_redis(
        self, mock_redis: AsyncMock,
    ) -> None:
        tracker = HybridConnectionTracker(
            pool_size=100,
            max_tenant_fraction=0.2,
            redis_conn=mock_redis,
            sync_interval=0.05,
        )
        await tracker.acquire("tenant-1")
        await tracker.start_sync()
        await asyncio.sleep(0.15)
        await tracker.stop_sync()
        mock_redis.set.assert_called()

    @pytest.mark.asyncio
    async def test_redis_unavailable_does_not_block(
        self, mock_redis: AsyncMock,
    ) -> None:
        mock_redis.set = AsyncMock(side_effect=ConnectionError("redis down"))
        tracker = HybridConnectionTracker(
            pool_size=100,
            max_tenant_fraction=0.2,
            redis_conn=mock_redis,
            sync_interval=0.05,
        )
        await tracker.acquire("tenant-1")
        await tracker.start_sync()
        await asyncio.sleep(0.15)
        await tracker.stop_sync()
        count = await tracker.active_count("tenant-1")
        assert count == 1


class TestCreateConnectionTrackerHybrid:
    def test_returns_hybrid_when_redis_and_hybrid_mode(self) -> None:
        redis = AsyncMock()
        tracker = create_connection_tracker(
            pool_size=100,
            redis_conn=redis,
            mode="hybrid",
        )
        assert isinstance(tracker, HybridConnectionTracker)

    def test_returns_local_without_redis(self) -> None:
        tracker = create_connection_tracker(pool_size=100)
        assert isinstance(tracker, TenantConnectionTracker)
