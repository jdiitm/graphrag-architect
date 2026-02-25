from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol

logger = logging.getLogger(__name__)


class IsolationMode(Enum):
    LOGICAL = "logical"
    PHYSICAL = "physical"


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

    def __post_init__(self) -> None:
        if self.isolation_mode == IsolationMode.LOGICAL:
            logger.warning(
                "TenantContext for %s uses LOGICAL isolation. "
                "LOGICAL mode is not recommended for production or "
                "compliance-sensitive environments (SOC2/FedRAMP). "
                "Use PHYSICAL isolation with dedicated databases.",
                self.tenant_id,
            )

    @classmethod
    def from_config(cls, config: TenantConfig) -> TenantContext:
        return cls(
            tenant_id=config.tenant_id,
            isolation_mode=config.isolation_mode,
            database_name=config.database_name,
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
