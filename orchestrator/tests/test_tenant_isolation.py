from __future__ import annotations

import ast
import os

import pytest

from orchestrator.app.tenant_isolation import (
    IsolationMode,
    TenantAwareDriverPool,
    TenantConfig,
    TenantRegistry,
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


class TestTenantAwarePoolIntegration:

    def test_get_database_for_physical_tenant(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(
            tenant_id="bigcorp",
            isolation_mode=IsolationMode.PHYSICAL,
            database_name="bigcorp_db",
        ))
        cfg = registry.get("bigcorp")
        assert cfg is not None
        assert cfg.database_name == "bigcorp_db"

    def test_get_database_for_logical_tenant_uses_default(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(tenant_id="smallco"))
        cfg = registry.get("smallco")
        assert cfg is not None
        assert cfg.database_name == "neo4j"

    def test_pool_resolves_database_for_tenant(self) -> None:
        from orchestrator.app.neo4j_pool import resolve_database_for_tenant

        registry = TenantRegistry()
        registry.register(TenantConfig(
            tenant_id="acme",
            isolation_mode=IsolationMode.PHYSICAL,
            database_name="acme_db",
        ))
        assert resolve_database_for_tenant(registry, "acme") == "acme_db"
        assert resolve_database_for_tenant(registry, "unknown") == "neo4j"

    def test_pool_resolves_logical_to_default(self) -> None:
        from orchestrator.app.neo4j_pool import resolve_database_for_tenant

        registry = TenantRegistry()
        registry.register(TenantConfig(tenant_id="smallco"))
        assert resolve_database_for_tenant(registry, "smallco") == "neo4j"


class TestDeadCodeRemoved:

    def test_inject_tenant_filter_not_exported(self) -> None:
        import orchestrator.app.tenant_isolation as mod
        assert not hasattr(mod, "inject_tenant_filter")

    def test_build_tenant_params_not_exported(self) -> None:
        import orchestrator.app.tenant_isolation as mod
        assert not hasattr(mod, "build_tenant_params")


class TestNoStringBasedCypherManipulation:

    _SOURCE_DIR = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "app",
    )

    def _scan_source_for_find_calls(self, forbidden_args):
        violations = []
        for filename in os.listdir(self._SOURCE_DIR):
            if not filename.endswith(".py"):
                continue
            filepath = os.path.join(self._SOURCE_DIR, filename)
            with open(filepath, encoding="utf-8") as fh:
                try:
                    tree = ast.parse(fh.read(), filename)
                except SyntaxError:
                    continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                func = node.func
                if not (isinstance(func, ast.Attribute) and func.attr == "find"):
                    continue
                if not node.args:
                    continue
                arg = node.args[0]
                if isinstance(arg, ast.Constant) and arg.value in forbidden_args:
                    violations.append(
                        f"{filename}:{node.lineno} .find({arg.value!r})"
                    )
        return violations

    def test_no_string_find_where_in_source(self) -> None:
        violations = self._scan_source_for_find_calls({"WHERE", "RETURN"})
        assert violations == [], (
            f"String-based Cypher manipulation detected: {violations}"
        )
