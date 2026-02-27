from __future__ import annotations

import ast
import logging
import logging.handlers
import os

import pytest

from orchestrator.app.tenant_isolation import (
    IsolationMode,
    TenantConfig,
    TenantContext,
    TenantRegistry,
    TenantRouter,
)


class TestTenantConfig:

    def test_defaults_to_physical_isolation(self) -> None:
        config = TenantConfig(tenant_id="acme")
        assert config.isolation_mode == IsolationMode.PHYSICAL, (
            "TenantConfig must default to PHYSICAL isolation for production safety. "
            f"Got: {config.isolation_mode}"
        )
        assert config.database_name == "neo4j"
        assert config.max_concurrent_queries == 50

    def test_tenant_context_defaults_to_physical(self) -> None:
        ctx = TenantContext.default(tenant_id="acme")
        assert ctx.isolation_mode == IsolationMode.PHYSICAL, (
            "TenantContext.default() must use PHYSICAL isolation. "
            f"Got: {ctx.isolation_mode}"
        )

    def test_logical_mode_requires_explicit_opt_in(self) -> None:
        config = TenantConfig(
            tenant_id="dev-tenant",
            isolation_mode=IsolationMode.LOGICAL,
        )
        assert config.isolation_mode == IsolationMode.LOGICAL

    def test_logical_mode_emits_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            TenantContext(
                tenant_id="risky-tenant",
                isolation_mode=IsolationMode.LOGICAL,
            )
        warning_msgs = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("LOGICAL" in msg for msg in warning_msgs), (
            "Creating a TenantContext with LOGICAL isolation must emit a WARNING log. "
            f"Log records: {warning_msgs}"
        )

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


class TestTenantRouterInline:

    def test_returns_default_database_for_physical_default(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(tenant_id="acme"))
        router = TenantRouter(registry)
        assert router.resolve_database("acme") == "neo4j"

    def test_returns_custom_database_for_physical(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(
            tenant_id="bigcorp",
            isolation_mode=IsolationMode.PHYSICAL,
            database_name="bigcorp_db",
        ))
        router = TenantRouter(registry)
        assert router.resolve_database("bigcorp") == "bigcorp_db"

    def test_returns_default_for_unknown(self) -> None:
        registry = TenantRegistry()
        router = TenantRouter(registry)
        assert router.resolve_database("unknown") == "neo4j"


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

    def test_get_database_for_default_tenant_uses_default(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(tenant_id="smallco"))
        cfg = registry.get("smallco")
        assert cfg is not None
        assert cfg.database_name == "neo4j"

    def test_pool_resolves_database_for_tenant(self) -> None:
        from orchestrator.app.neo4j_pool import (
            UnknownTenantError,
            resolve_database_for_tenant,
        )

        registry = TenantRegistry()
        registry.register(TenantConfig(
            tenant_id="acme",
            isolation_mode=IsolationMode.PHYSICAL,
            database_name="acme_db",
        ))
        assert resolve_database_for_tenant(registry, "acme") == "acme_db"
        with pytest.raises(UnknownTenantError):
            resolve_database_for_tenant(registry, "unknown")

    def test_pool_resolves_default_to_default_db(self) -> None:
        from orchestrator.app.neo4j_pool import resolve_database_for_tenant

        registry = TenantRegistry()
        registry.register(TenantConfig(tenant_id="smallco"))
        assert resolve_database_for_tenant(registry, "smallco") == "neo4j"


class TestProductionIsolationEnforcement:

    def test_logical_mode_rejected_in_production(self) -> None:
        from orchestrator.app.tenant_isolation import (
            LogicalIsolationInProductionError,
            TenantContext,
        )
        with pytest.raises(LogicalIsolationInProductionError):
            TenantContext(
                tenant_id="acme",
                isolation_mode=IsolationMode.LOGICAL,
                database_name="neo4j",
                deployment_mode="production",
            )

    def test_logical_mode_allowed_in_dev(self) -> None:
        from orchestrator.app.tenant_isolation import TenantContext
        ctx = TenantContext(
            tenant_id="dev-tenant",
            isolation_mode=IsolationMode.LOGICAL,
            database_name="neo4j",
            deployment_mode="dev",
        )
        assert ctx.isolation_mode == IsolationMode.LOGICAL

    def test_physical_mode_allowed_in_production(self) -> None:
        from orchestrator.app.tenant_isolation import TenantContext
        ctx = TenantContext(
            tenant_id="prod-tenant",
            isolation_mode=IsolationMode.PHYSICAL,
            database_name="prod_db",
            deployment_mode="production",
        )
        assert ctx.isolation_mode == IsolationMode.PHYSICAL


class TestTenantAuditLogger:

    def test_audit_logger_emits_structured_log(self) -> None:
        import json
        import logging
        from orchestrator.app.tenant_isolation import StructuredTenantAuditLogger

        handler = logging.handlers.MemoryHandler(capacity=100)
        audit_logger = StructuredTenantAuditLogger()
        audit_logger._logger.addHandler(handler)
        audit_logger._logger.setLevel(logging.INFO)

        audit_logger.log_query(
            tenant_id="acme",
            query_hash="abc123",
            result_count=42,
        )

        handler.flush()
        assert len(handler.buffer) >= 1
        record = handler.buffer[-1]
        assert "acme" in record.getMessage()
        audit_logger._logger.removeHandler(handler)

    def test_audit_logger_includes_all_fields(self) -> None:
        import json
        import logging
        from orchestrator.app.tenant_isolation import StructuredTenantAuditLogger

        captured: list[str] = []

        class _CaptureHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                captured.append(record.getMessage())

        audit_logger = StructuredTenantAuditLogger()
        handler = _CaptureHandler()
        audit_logger._logger.addHandler(handler)
        audit_logger._logger.setLevel(logging.INFO)

        audit_logger.log_query(
            tenant_id="bigcorp",
            query_hash="def456",
            result_count=10,
        )

        assert len(captured) == 1
        parsed = json.loads(captured[0])
        assert parsed["tenant_id"] == "bigcorp"
        assert parsed["query_hash"] == "def456"
        assert parsed["result_count"] == 10
        assert "timestamp" in parsed
        audit_logger._logger.removeHandler(handler)


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
