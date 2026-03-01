from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    GlobalProviderBreaker,
    InMemoryStateStore,
    RedisStateStore,
    StateStore,
    TenantCircuitBreakerRegistry,
)


class TestTenantRegistryWithStore:

    @pytest.mark.asyncio
    async def test_registry_propagates_store_to_breakers(self) -> None:
        store = InMemoryStateStore()
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(),
            name_prefix="test",
            store=store,
        )
        breaker = await registry.for_tenant("tenant-1")
        assert breaker._store is store

    @pytest.mark.asyncio
    async def test_registry_without_store_uses_local_default(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(),
            name_prefix="test",
        )
        breaker = await registry.for_tenant("tenant-1")
        assert isinstance(breaker._store, InMemoryStateStore)

    @pytest.mark.asyncio
    async def test_all_tenants_share_same_store_instance(self) -> None:
        store = InMemoryStateStore()
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(),
            name_prefix="shared",
            store=store,
        )
        b1 = await registry.for_tenant("t1")
        b2 = await registry.for_tenant("t2")
        assert b1._store is b2._store is store


class TestGlobalProviderBreakerWithStore:

    @pytest.mark.asyncio
    async def test_global_breaker_propagates_store(self) -> None:
        store = InMemoryStateStore()
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(),
            name_prefix="llm",
            store=store,
        )
        gpb = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(),
            store=store,
        )
        assert gpb._global._store is store

    @pytest.mark.asyncio
    async def test_global_breaker_without_store_uses_local(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(),
            name_prefix="llm",
        )
        gpb = GlobalProviderBreaker(registry=registry)
        assert isinstance(gpb._global._store, InMemoryStateStore)


class TestQueryEngineBreakersUseStore:

    def test_build_query_breakers_uses_redis_when_available(self) -> None:
        from orchestrator.app.query_engine import build_query_breakers
        store = InMemoryStateStore()
        llm_breaker, embed_breaker = build_query_breakers(store=store)
        assert llm_breaker._global._store is store
        assert embed_breaker._global._store is store

    def test_build_query_breakers_defaults_to_local(self) -> None:
        from orchestrator.app.query_engine import build_query_breakers
        llm_breaker, embed_breaker = build_query_breakers()
        assert isinstance(llm_breaker._global._store, InMemoryStateStore)
        assert isinstance(embed_breaker._global._store, InMemoryStateStore)
