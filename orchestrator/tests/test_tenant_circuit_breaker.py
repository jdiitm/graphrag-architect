from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    TenantCircuitBreakerRegistry,
)


class TestForTenantReturnsCircuitBreaker:
    @pytest.mark.asyncio
    async def test_returns_circuit_breaker_instance(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=3, recovery_timeout=30.0),
            name_prefix="test",
        )
        breaker = await registry.for_tenant("tenant-alpha")
        assert isinstance(breaker, CircuitBreaker)

    @pytest.mark.asyncio
    async def test_same_tenant_returns_same_breaker(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(),
            name_prefix="test",
        )
        first = await registry.for_tenant("tenant-1")
        second = await registry.for_tenant("tenant-1")
        assert first is second


class TestTenantIsolation:
    @pytest.mark.asyncio
    async def test_tenant_a_failures_do_not_open_tenant_b(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=2, recovery_timeout=30.0),
            name_prefix="isolation",
        )
        breaker_a = await registry.for_tenant("tenant-a")
        breaker_b = await registry.for_tenant("tenant-b")

        failing = AsyncMock(side_effect=ConnectionError("boom"))
        for _ in range(2):
            with pytest.raises(ConnectionError):
                await breaker_a.call(failing)

        with pytest.raises(CircuitOpenError):
            await breaker_a.call(AsyncMock(return_value="ok"))

        result = await breaker_b.call(AsyncMock(return_value="healthy"))
        assert result == "healthy"


class TestLRUEviction:
    @pytest.mark.asyncio
    async def test_oldest_tenant_evicted_at_capacity(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(),
            name_prefix="evict",
            max_tenants=3,
        )
        await registry.for_tenant("t1")
        await registry.for_tenant("t2")
        await registry.for_tenant("t3")

        first_t1 = await registry.for_tenant("t1")
        await registry.for_tenant("t1")

        await registry.for_tenant("t4")

        new_t2 = await registry.for_tenant("t2")
        assert new_t2 is not first_t1

    @pytest.mark.asyncio
    async def test_recently_used_not_evicted(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(),
            name_prefix="lru",
            max_tenants=3,
        )
        first_t1 = await registry.for_tenant("t1")
        await registry.for_tenant("t2")
        await registry.for_tenant("t3")

        refreshed_t1 = await registry.for_tenant("t1")
        assert refreshed_t1 is first_t1

        await registry.for_tenant("t4")

        still_t1 = await registry.for_tenant("t1")
        assert still_t1 is first_t1


class TestSameConfigPerTenant:
    @pytest.mark.asyncio
    async def test_all_tenants_share_identical_config(self) -> None:
        config = CircuitBreakerConfig(
            failure_threshold=7, recovery_timeout=42.0, half_open_max_calls=2,
        )
        registry = TenantCircuitBreakerRegistry(
            config=config, name_prefix="cfg",
        )
        breaker_a = await registry.for_tenant("alpha")
        breaker_b = await registry.for_tenant("beta")

        assert breaker_a._config is config
        assert breaker_b._config is config


class TestRegistryThreadSafety:
    @pytest.mark.asyncio
    async def test_concurrent_for_tenant_no_corruption(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(),
            name_prefix="safe",
            max_tenants=50,
        )
        tenant_ids = [f"tenant-{i}" for i in range(100)]

        async def _get(tid: str) -> CircuitBreaker:
            return await registry.for_tenant(tid)

        results = await asyncio.gather(*[_get(tid) for tid in tenant_ids])
        assert all(isinstance(r, CircuitBreaker) for r in results)

        unique_ids = {id(await registry.for_tenant(f"tenant-{i}")) for i in range(50)}
        assert len(unique_ids) == 50
