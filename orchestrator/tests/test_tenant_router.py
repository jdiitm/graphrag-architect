from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.tenant_isolation import (
    IsolationMode,
    TenantConfig,
    TenantRegistry,
    TenantRouter,
)


class TestTenantRouter:
    def test_logical_tenant_returns_default_database(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(tenant_id="acme"))
        router = TenantRouter(registry)
        assert router.resolve_database("acme") == "neo4j"

    def test_physical_tenant_returns_custom_database(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(
            tenant_id="bigcorp",
            isolation_mode=IsolationMode.PHYSICAL,
            database_name="bigcorp_db",
        ))
        router = TenantRouter(registry)
        assert router.resolve_database("bigcorp") == "bigcorp_db"

    def test_unknown_tenant_returns_default(self) -> None:
        registry = TenantRegistry()
        router = TenantRouter(registry)
        assert router.resolve_database("unknown") == "neo4j"

    def test_session_kwargs_include_database(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(
            tenant_id="bigcorp",
            isolation_mode=IsolationMode.PHYSICAL,
            database_name="bigcorp_db",
        ))
        router = TenantRouter(registry)
        kwargs = router.session_kwargs("bigcorp")
        assert kwargs["database"] == "bigcorp_db"

    def test_session_kwargs_logical_omits_database(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(tenant_id="acme"))
        router = TenantRouter(registry)
        kwargs = router.session_kwargs("acme")
        assert kwargs.get("database", "neo4j") == "neo4j"


class TestTenantAwareDriverPoolRemoved:
    def test_no_driver_pool_class(self) -> None:
        import orchestrator.app.tenant_isolation as mod

        assert not hasattr(mod, "TenantAwareDriverPool"), (
            "TenantAwareDriverPool should be removed"
        )

    def test_no_default_max_tenant_pools(self) -> None:
        import orchestrator.app.tenant_isolation as mod

        assert not hasattr(mod, "default_max_tenant_pools"), (
            "default_max_tenant_pools should be removed"
        )
