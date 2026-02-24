from __future__ import annotations

from typing import Optional

from neo4j import AsyncDriver, AsyncGraphDatabase

from orchestrator.app.config import Neo4jConfig
from orchestrator.app.tenant_isolation import (
    IsolationMode,
    TenantAwareDriverPool,
    TenantRegistry,
)

_state: dict[str, Optional[AsyncDriver | float | str]] = {
    "driver": None,
    "query_timeout": None,
    "database": None,
}

_TENANT_STATE: dict[str, Optional[TenantRegistry | TenantAwareDriverPool]] = {
    "registry": None,
    "pool": None,
}


def init_driver() -> None:
    config = Neo4jConfig.from_env()
    _state["driver"] = AsyncGraphDatabase.driver(
        config.uri,
        auth=(config.username, config.password),
        max_transaction_retry_time=config.query_timeout,
    )
    _state["query_timeout"] = config.query_timeout
    _state["database"] = config.database


async def close_driver() -> None:
    driver = _state.get("driver")
    if driver is not None:
        await driver.close()
        _state["driver"] = None
    pool = _TENANT_STATE.get("pool")
    if pool is not None and isinstance(pool, TenantAwareDriverPool):
        await pool.close_all()


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


def get_tenant_registry() -> Optional[TenantRegistry]:
    reg = _TENANT_STATE.get("registry")
    if reg is not None and isinstance(reg, TenantRegistry):
        return reg
    return None


def set_tenant_registry(registry: TenantRegistry) -> None:
    _TENANT_STATE["registry"] = registry


def get_tenant_pool() -> Optional[TenantAwareDriverPool]:
    pool = _TENANT_STATE.get("pool")
    if pool is not None and isinstance(pool, TenantAwareDriverPool):
        return pool
    return None


def set_tenant_pool(pool: TenantAwareDriverPool) -> None:
    _TENANT_STATE["pool"] = pool


def resolve_database_for_tenant(
    registry: Optional[TenantRegistry],
    tenant_id: str,
    default_database: str = "neo4j",
) -> str:
    if registry is None:
        return default_database
    cfg = registry.get(tenant_id)
    if cfg is None:
        return default_database
    if cfg.isolation_mode == IsolationMode.PHYSICAL:
        return cfg.database_name
    return default_database
