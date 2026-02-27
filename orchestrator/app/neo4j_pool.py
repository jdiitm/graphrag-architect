from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional, Tuple

from neo4j import AsyncDriver, AsyncGraphDatabase

from orchestrator.app.config import Neo4jConfig
from orchestrator.app.tenant_isolation import (
    IsolationMode,
    TenantRegistry,
    TenantRouter,
)


class UnknownTenantError(LookupError):
    pass


class RegistryUnavailableError(RuntimeError):
    pass

_state: dict[str, Optional[AsyncDriver | float | str]] = {
    "driver": None,
    "query_timeout": None,
    "database": None,
}

_TENANT_STATE: dict[str, Optional[TenantRegistry | TenantRouter]] = {
    "registry": None,
    "router": None,
}


def init_driver() -> None:
    config = Neo4jConfig.from_env()
    _state["driver"] = AsyncGraphDatabase.driver(
        config.uri,
        auth=(config.username, config.password),
        max_transaction_retry_time=config.query_timeout,
        max_connection_pool_size=config.max_connection_pool_size,
        connection_acquisition_timeout=config.connection_acquisition_timeout,
    )
    _state["query_timeout"] = config.query_timeout
    _state["database"] = config.database


class ReplicaAwarePool:
    def __init__(
        self,
        primary_driver: AsyncDriver,
        replica_drivers: tuple[AsyncDriver, ...] = (),
    ) -> None:
        self._primary = primary_driver
        self._replicas = replica_drivers
        self._index = 0

    def get_read_driver(self) -> AsyncDriver:
        if not self._replicas:
            return self._primary
        driver = self._replicas[self._index % len(self._replicas)]
        self._index += 1
        return driver

    def get_write_driver(self) -> AsyncDriver:
        return self._primary

    async def close_all(self) -> None:
        await self._primary.close()
        for replica in self._replicas:
            await replica.close()


_REPLICA_STATE: dict[str, Optional[ReplicaAwarePool]] = {
    "pool": None,
}


async def close_driver() -> None:
    driver = _state.get("driver")
    if driver is not None:
        await driver.close()
        _state["driver"] = None


def get_query_timeout() -> float:
    timeout = _state.get("query_timeout")
    if timeout is None:
        raise RuntimeError(
            "Neo4j driver not initialized. Call init_driver() first."
        )
    return timeout


def get_database() -> str:
    database = _state.get("database")
    if database is None:
        raise RuntimeError(
            "Neo4j driver not initialized. Call init_driver() first."
        )
    return database


def get_driver() -> AsyncDriver:
    driver = _state.get("driver")
    if driver is None:
        raise RuntimeError(
            "Neo4j driver not initialized. Call init_driver() first."
        )
    return driver


def get_read_driver() -> AsyncDriver:
    pool = _REPLICA_STATE.get("pool")
    if pool is not None:
        return pool.get_read_driver()
    return get_driver()


def get_tenant_registry() -> Optional[TenantRegistry]:
    reg = _TENANT_STATE.get("registry")
    if reg is not None and isinstance(reg, TenantRegistry):
        return reg
    return None


def set_tenant_registry(registry: TenantRegistry) -> None:
    _TENANT_STATE["registry"] = registry


def get_tenant_router() -> Optional[TenantRouter]:
    router = _TENANT_STATE.get("router")
    if router is not None and isinstance(router, TenantRouter):
        return router
    return None


def set_tenant_router(router: TenantRouter) -> None:
    _TENANT_STATE["router"] = router


def resolve_database_for_tenant(
    registry: Optional[TenantRegistry],
    tenant_id: str,
    default_database: str = "neo4j",
) -> str:
    if registry is None:
        raise RegistryUnavailableError(
            "Tenant registry not configured — cannot resolve tenant "
            f"{tenant_id!r}. All tenants must be explicitly registered."
        )
    cfg = registry.get(tenant_id)
    if cfg is None:
        raise UnknownTenantError(
            f"Tenant {tenant_id!r} is not registered. Fail-closed: "
            f"refusing to route to default database."
        )
    if cfg.isolation_mode == IsolationMode.PHYSICAL:
        return cfg.database_name
    return default_database


def resolve_driver_for_tenant(
    registry: Optional[TenantRegistry],
    tenant_id: str,
) -> Tuple[Any, str]:
    if not tenant_id:
        return get_driver(), get_database()
    database = resolve_database_for_tenant(registry, tenant_id)
    return get_driver(), database


class TenantQuotaExceededError(RuntimeError):
    pass


class TenantConnectionTracker:
    def __init__(
        self,
        pool_size: int,
        max_tenant_fraction: float = 0.2,
    ) -> None:
        self._pool_size = pool_size
        self._max_per_tenant = max(1, int(pool_size * max_tenant_fraction))
        self._active: Dict[str, int] = {}
        self._lock = asyncio.Lock()

    @property
    def max_per_tenant(self) -> int:
        return self._max_per_tenant

    def active_count(self, tenant_id: str) -> int:
        return self._active.get(tenant_id, 0)

    async def acquire(self, tenant_id: str) -> None:
        async with self._lock:
            current = self._active.get(tenant_id, 0)
            if current >= self._max_per_tenant:
                raise TenantQuotaExceededError(
                    f"Tenant {tenant_id!r} exceeds connection quota "
                    f"({current}/{self._max_per_tenant})"
                )
            self._active[tenant_id] = current + 1

    async def release(self, tenant_id: str) -> None:
        async with self._lock:
            current = self._active.get(tenant_id, 0)
            if current <= 1:
                self._active.pop(tenant_id, None)
            else:
                self._active[tenant_id] = current - 1


_TRACKER_STATE: Dict[str, Optional[TenantConnectionTracker]] = {
    "tracker": None,
}


def init_tenant_tracker(
    pool_size: int = 100,
    max_tenant_fraction: float = 0.2,
) -> TenantConnectionTracker:
    tracker = TenantConnectionTracker(
        pool_size=pool_size, max_tenant_fraction=max_tenant_fraction,
    )
    _TRACKER_STATE["tracker"] = tracker
    return tracker


def get_tenant_tracker() -> Optional[TenantConnectionTracker]:
    return _TRACKER_STATE.get("tracker")
