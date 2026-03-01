from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol

logger = logging.getLogger(__name__)
_audit_logger = logging.getLogger("graphrag.tenant_audit")


class TenantIsolationViolation(Exception):
    pass


class IsolationMode(Enum):
    LOGICAL = "logical"
    PHYSICAL = "physical"


class LogicalIsolationInProductionError(Exception):
    pass


@dataclass(frozen=True)
class TenantConfig:
    tenant_id: str
    isolation_mode: IsolationMode = IsolationMode.PHYSICAL
    database_name: str = "neo4j"
    label_prefix: str = ""
    max_concurrent_queries: int = 50


@dataclass(frozen=True)
class TenantContext:
    tenant_id: str
    isolation_mode: IsolationMode = IsolationMode.PHYSICAL
    database_name: str = "neo4j"
    deployment_mode: str = "dev"

    def __post_init__(self) -> None:
        if self.isolation_mode == IsolationMode.LOGICAL:
            if self.deployment_mode == "production":
                raise LogicalIsolationInProductionError(
                    f"LOGICAL isolation is forbidden in production for "
                    f"tenant {self.tenant_id!r}. Use PHYSICAL isolation "
                    f"with dedicated databases for SOC2/FedRAMP compliance."
                )
            logger.warning(
                "TenantContext for %s uses LOGICAL isolation. "
                "LOGICAL mode is not recommended for production or "
                "compliance-sensitive environments (SOC2/FedRAMP). "
                "Use PHYSICAL isolation with dedicated databases.",
                self.tenant_id,
            )

    @classmethod
    def from_config(
        cls, config: TenantConfig, deployment_mode: str = "dev",
    ) -> TenantContext:
        return cls(
            tenant_id=config.tenant_id,
            isolation_mode=config.isolation_mode,
            database_name=config.database_name,
            deployment_mode=deployment_mode,
        )

    @classmethod
    def default(cls, tenant_id: str = "") -> TenantContext:
        return cls(tenant_id=tenant_id)


class TenantDriver(Protocol):
    async def session(self, **kwargs: Any) -> Any: ...
    async def close(self) -> None: ...


class TenantRegistry:
    def __init__(self) -> None:
        self._tenants: Dict[str, TenantConfig] = {}

    def register(self, config: TenantConfig) -> None:
        if config.tenant_id in self._tenants:
            raise ValueError(f"Tenant already registered: {config.tenant_id}")
        self._tenants[config.tenant_id] = config
        logger.info("Registered tenant: %s mode=%s", config.tenant_id, config.isolation_mode.value)

    def get(self, tenant_id: str) -> Optional[TenantConfig]:
        return self._tenants.get(tenant_id)

    def all_tenants(self) -> List[TenantConfig]:
        return list(self._tenants.values())

    def remove(self, tenant_id: str) -> bool:
        if tenant_id in self._tenants:
            del self._tenants[tenant_id]
            return True
        return False


class TenantAuditLogger(Protocol):
    def log_query(
        self,
        tenant_id: str,
        query_hash: str,
        result_count: int,
    ) -> None: ...


class StructuredTenantAuditLogger:
    def __init__(self) -> None:
        self._logger = logging.getLogger("graphrag.tenant_audit")

    def log_query(
        self,
        tenant_id: str,
        query_hash: str,
        result_count: int,
    ) -> None:
        entry = json.dumps({
            "tenant_id": tenant_id,
            "query_hash": query_hash,
            "result_count": result_count,
            "timestamp": time.time(),
        })
        self._logger.info(entry)


_DEFAULT_DATABASE = "neo4j"


class TenantRouter:
    def __init__(self, registry: TenantRegistry) -> None:
        self._registry = registry

    def resolve_database(self, tenant_id: str) -> str:
        config = self._registry.get(tenant_id)
        if config is None:
            return _DEFAULT_DATABASE
        return config.database_name

    def session_kwargs(self, tenant_id: str) -> Dict[str, Any]:
        db = self.resolve_database(tenant_id)
        return {"database": db}

    def get_connection(
        self, tenant_id: str, driver: Any,
    ) -> TenantConnectionWrapper:
        config = self._registry.get(tenant_id)
        if config is None:
            raise LookupError(
                f"Tenant {tenant_id!r} not registered; "
                f"cannot issue a bound connection"
            )
        return TenantConnectionWrapper(
            driver=driver,
            bound_tenant_id=tenant_id,
            bound_database=config.database_name,
        )


class TenantConnectionWrapper:
    def __init__(
        self,
        driver: Any,
        bound_tenant_id: str,
        bound_database: str,
    ) -> None:
        self._driver = driver
        self._bound_tenant_id = bound_tenant_id
        self._bound_database = bound_database

    @property
    def bound_tenant_id(self) -> str:
        return self._bound_tenant_id

    @property
    def bound_database(self) -> str:
        return self._bound_database

    @property
    def driver(self) -> Any:
        return self._driver

    def validate_query_tenant(self, tenant_id: str) -> None:
        if tenant_id != self._bound_tenant_id:
            raise TenantIsolationViolation(
                f"Connection bound to tenant {self._bound_tenant_id!r} "
                f"but query targets tenant {tenant_id!r}; "
                f"cross-tenant data access blocked"
            )

    def validate_database(self, database: str) -> None:
        if database != self._bound_database:
            raise TenantIsolationViolation(
                f"Connection bound to database {self._bound_database!r} "
                f"but query targets database {database!r}; "
                f"cross-database access blocked"
            )


def validate_tenant_binding(
    connection: Any,
    tenant_id: str,
    expected_database: Optional[str] = None,
) -> None:
    if not isinstance(connection, TenantConnectionWrapper):
        raise TypeError(
            f"Expected TenantConnectionWrapper, got {type(connection).__name__}"
        )
    connection.validate_query_tenant(tenant_id)
    if expected_database is not None:
        connection.validate_database(expected_database)


async def detect_neo4j_edition(driver: Any) -> str:
    async with driver.session() as session:
        result = await session.run(
            "CALL dbms.components() YIELD edition RETURN edition"
        )
        record = await result.single()
        return str(record["edition"]).lower()


async def validate_physical_isolation_support(
    driver: Any,
    registry: TenantRegistry,
) -> None:
    tenants_requiring_physical = [
        cfg for cfg in registry.all_tenants()
        if cfg.isolation_mode == IsolationMode.PHYSICAL
    ]
    if not tenants_requiring_physical:
        return

    edition = await detect_neo4j_edition(driver)
    if edition == "community":
        tenant_ids = [cfg.tenant_id for cfg in tenants_requiring_physical]
        raise TenantIsolationViolation(
            f"Neo4j Community edition does not support multi-database. "
            f"Tenants {tenant_ids} request PHYSICAL isolation which "
            f"requires Neo4j Enterprise or Aura. Either upgrade Neo4j "
            f"or switch these tenants to LOGICAL isolation."
        )


class OrphanedPoolDetector:
    def __init__(
        self,
        tenant_registry: TenantRegistry,
        pool_registry: Dict[str, Any],
    ) -> None:
        self._tenant_registry = tenant_registry
        self._pool_registry = pool_registry

    def check(self) -> List[str]:
        active_tenant_ids = {
            cfg.tenant_id for cfg in self._tenant_registry.all_tenants()
        }
        orphaned: List[str] = []
        for pool_tenant_id in self._pool_registry:
            if pool_tenant_id not in active_tenant_ids:
                orphaned.append(pool_tenant_id)
                logger.warning(
                    "Orphaned connection pool detected for tenant %r",
                    pool_tenant_id,
                )
        return sorted(orphaned)
