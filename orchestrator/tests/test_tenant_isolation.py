from __future__ import annotations

import pytest

from orchestrator.app.tenant_isolation import (
    IsolationMode,
    TenantAwareDriverPool,
    TenantConfig,
    TenantRegistry,
    build_tenant_params,
    inject_tenant_filter,
)


class TestTenantConfig:

    def test_defaults(self) -> None:
        cfg = TenantConfig(tenant_id="acme")
        assert cfg.isolation_mode == IsolationMode.LOGICAL
        assert cfg.database_name == "neo4j"
        assert cfg.max_concurrent_queries == 50

    def test_physical_mode(self) -> None:
        cfg = TenantConfig(
            tenant_id="bigcorp",
            isolation_mode=IsolationMode.PHYSICAL,
            database_name="bigcorp_db",
        )
        assert cfg.isolation_mode == IsolationMode.PHYSICAL
        assert cfg.database_name == "bigcorp_db"


class TestTenantRegistry:

    def test_register_and_get(self) -> None:
        registry = TenantRegistry()
        cfg = TenantConfig(tenant_id="acme")
        registry.register(cfg)
        assert registry.get("acme") == cfg

    def test_get_missing_returns_none(self) -> None:
        registry = TenantRegistry()
        assert registry.get("nonexistent") is None

    def test_duplicate_rejected(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(tenant_id="acme"))
        with pytest.raises(ValueError, match="already registered"):
            registry.register(TenantConfig(tenant_id="acme"))

    def test_all_tenants(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(tenant_id="a"))
        registry.register(TenantConfig(tenant_id="b"))
        assert len(registry.all_tenants()) == 2

    def test_remove(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(tenant_id="acme"))
        assert registry.remove("acme") is True
        assert registry.get("acme") is None
        assert registry.remove("acme") is False


class TestTenantFilterInjection:

    def test_inject_into_where_clause(self) -> None:
        cypher = "MATCH (n:Service) WHERE n.name = $name RETURN n"
        result = inject_tenant_filter(cypher, "acme")
        assert "n.tenant_id = $__tenant_id" in result
        assert result.index("n.tenant_id") < result.index("n.name")

    def test_inject_without_where(self) -> None:
        cypher = "MATCH (n:Service) RETURN n"
        result = inject_tenant_filter(cypher, "acme")
        assert "WHERE n.tenant_id = $__tenant_id" in result

    def test_custom_alias(self) -> None:
        cypher = "MATCH (svc:Service) WHERE svc.name = $name RETURN svc"
        result = inject_tenant_filter(cypher, "acme", alias="svc")
        assert "svc.tenant_id = $__tenant_id" in result

    def test_build_params(self) -> None:
        params = build_tenant_params("acme")
        assert params == {"__tenant_id": "acme"}


class TestTenantAwareDriverPool:

    def test_returns_default_for_unknown_tenant(self) -> None:
        default = object()
        pool = TenantAwareDriverPool(default)
        assert pool.get_driver("unknown") is default

    def test_returns_tenant_specific_driver(self) -> None:
        default = object()
        tenant_driver = object()
        pool = TenantAwareDriverPool(default)
        pool.register_driver("acme", tenant_driver)
        assert pool.get_driver("acme") is tenant_driver

    @pytest.mark.asyncio
    async def test_close_all(self) -> None:
        class FakeDriver:
            closed = False
            async def close(self) -> None:
                self.closed = True

        default = object()
        pool = TenantAwareDriverPool(default)
        d1 = FakeDriver()
        d2 = FakeDriver()
        pool.register_driver("a", d1)
        pool.register_driver("b", d2)
        await pool.close_all()
        assert d1.closed
        assert d2.closed
