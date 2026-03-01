from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from orchestrator.app.neo4j_pool import (
    TenantConnectionTracker,
    TenantQuotaExceededError,
    init_tenant_tracker,
    get_tenant_tracker,
)


class TestTenantConnectionTrackerQuota:

    @pytest.mark.asyncio
    async def test_acquire_succeeds_under_quota(self) -> None:
        tracker = TenantConnectionTracker(pool_size=100, max_tenant_fraction=0.2)
        await tracker.acquire("tenant-a")
        assert await tracker.active_count("tenant-a") == 1

    @pytest.mark.asyncio
    async def test_acquire_fails_at_quota(self) -> None:
        tracker = TenantConnectionTracker(pool_size=10, max_tenant_fraction=0.2)
        await tracker.acquire("tenant-a")
        await tracker.acquire("tenant-a")
        with pytest.raises(TenantQuotaExceededError, match="quota"):
            await tracker.acquire("tenant-a")

    @pytest.mark.asyncio
    async def test_release_frees_slot(self) -> None:
        tracker = TenantConnectionTracker(pool_size=10, max_tenant_fraction=0.2)
        await tracker.acquire("tenant-a")
        await tracker.acquire("tenant-a")
        await tracker.release("tenant-a")
        assert await tracker.active_count("tenant-a") == 1
        await tracker.acquire("tenant-a")
        assert await tracker.active_count("tenant-a") == 2

    @pytest.mark.asyncio
    async def test_release_below_zero_clamps(self) -> None:
        tracker = TenantConnectionTracker(pool_size=10, max_tenant_fraction=0.2)
        await tracker.release("nonexistent-tenant")
        assert await tracker.active_count("nonexistent-tenant") == 0

    @pytest.mark.asyncio
    async def test_different_tenants_have_independent_quotas(self) -> None:
        tracker = TenantConnectionTracker(pool_size=10, max_tenant_fraction=0.2)
        await tracker.acquire("tenant-a")
        await tracker.acquire("tenant-a")
        await tracker.acquire("tenant-b")
        assert await tracker.active_count("tenant-a") == 2
        assert await tracker.active_count("tenant-b") == 1


class TestTenantConnectionTrackerConfig:

    def test_max_per_tenant_is_fraction_of_pool(self) -> None:
        tracker = TenantConnectionTracker(pool_size=100, max_tenant_fraction=0.2)
        assert tracker.max_per_tenant == 20

    def test_max_per_tenant_rounds_down(self) -> None:
        tracker = TenantConnectionTracker(pool_size=7, max_tenant_fraction=0.2)
        assert tracker.max_per_tenant == 1

    def test_max_per_tenant_minimum_one(self) -> None:
        tracker = TenantConnectionTracker(pool_size=1, max_tenant_fraction=0.1)
        assert tracker.max_per_tenant >= 1


class TestTenantConnectionTrackerConcurrency:

    @pytest.mark.asyncio
    async def test_concurrent_acquires_respect_quota(self) -> None:
        tracker = TenantConnectionTracker(pool_size=10, max_tenant_fraction=0.2)
        results: list[bool] = []

        async def _try_acquire() -> bool:
            try:
                await tracker.acquire("tenant-a")
                return True
            except TenantQuotaExceededError:
                return False

        tasks = [asyncio.create_task(_try_acquire()) for _ in range(5)]
        results = await asyncio.gather(*tasks)
        successes = sum(1 for r in results if r)
        assert successes == tracker.max_per_tenant


class TestTrackerFactoryFunctions:

    def test_init_creates_tracker(self) -> None:
        tracker = init_tenant_tracker(pool_size=50, max_tenant_fraction=0.1)
        assert tracker.max_per_tenant == 5

    def test_get_returns_initialized_tracker(self) -> None:
        init_tenant_tracker(pool_size=50, max_tenant_fraction=0.1)
        result = get_tenant_tracker()
        assert result is not None
        assert result.max_per_tenant == 5
