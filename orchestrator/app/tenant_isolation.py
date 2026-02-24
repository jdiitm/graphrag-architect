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
    isolation_mode: IsolationMode = IsolationMode.LOGICAL
    database_name: str = "neo4j"
    label_prefix: str = ""
    max_concurrent_queries: int = 50


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


class TenantAwareDriverPool:
    def __init__(self, default_driver: Any) -> None:
        self._default_driver = default_driver
        self._drivers: Dict[str, Any] = {}

    def register_driver(self, tenant_id: str, driver: Any) -> None:
        self._drivers[tenant_id] = driver

    def get_driver(self, tenant_id: str) -> Any:
        driver = self._drivers.get(tenant_id)
        if driver is not None:
            return driver
        return self._default_driver

    async def close_all(self) -> None:
        for driver in self._drivers.values():
            if hasattr(driver, "close"):
                await driver.close()
        self._drivers.clear()
