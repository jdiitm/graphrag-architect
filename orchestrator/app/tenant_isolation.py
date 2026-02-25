from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol

logger = logging.getLogger(__name__)
_audit_logger = logging.getLogger("graphrag.tenant_audit")


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
