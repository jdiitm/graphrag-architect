from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.tenant_isolation import (
    IsolationMode,
    TenantConfig,
    TenantConnectionWrapper,
    TenantIsolationViolation,
    TenantRegistry,
    TenantRouter,
    OrphanedPoolDetector,
    validate_tenant_binding,
)


class TestTenantConnectionWrapperRejectsWrongTenant:

    def test_reject_wrong_tenant_id(self) -> None:
        mock_driver = MagicMock()
        wrapper = TenantConnectionWrapper(
            driver=mock_driver,
            bound_tenant_id="acme",
            bound_database="neo4j_acme",
        )
        with pytest.raises(TenantIsolationViolation, match="acme.*evil_corp"):
            wrapper.validate_query_tenant("evil_corp")

    def test_reject_empty_tenant_id(self) -> None:
        mock_driver = MagicMock()
        wrapper = TenantConnectionWrapper(
            driver=mock_driver,
            bound_tenant_id="acme",
            bound_database="neo4j_acme",
        )
        with pytest.raises(TenantIsolationViolation):
            wrapper.validate_query_tenant("")


class TestTenantConnectionWrapperAllowsCorrectTenant:

    def test_allow_matching_tenant_id(self) -> None:
        mock_driver = MagicMock()
        wrapper = TenantConnectionWrapper(
            driver=mock_driver,
            bound_tenant_id="acme",
            bound_database="neo4j_acme",
        )
        wrapper.validate_query_tenant("acme")

    def test_wrapper_exposes_bound_tenant(self) -> None:
        mock_driver = MagicMock()
        wrapper = TenantConnectionWrapper(
            driver=mock_driver,
            bound_tenant_id="acme",
            bound_database="neo4j_acme",
        )
        assert wrapper.bound_tenant_id == "acme"
        assert wrapper.bound_database == "neo4j_acme"


class TestConnectionCheckoutValidatesBinding:

    def test_router_get_connection_returns_bound_wrapper(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(
            tenant_id="acme",
            isolation_mode=IsolationMode.PHYSICAL,
            database_name="neo4j_acme",
        ))
        mock_driver = MagicMock()
        router = TenantRouter(registry)

        wrapper = router.get_connection("acme", mock_driver)

        assert isinstance(wrapper, TenantConnectionWrapper)
        assert wrapper.bound_tenant_id == "acme"
        assert wrapper.bound_database == "neo4j_acme"

    def test_router_get_connection_validates_on_dispatch(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(
            tenant_id="acme",
            isolation_mode=IsolationMode.PHYSICAL,
            database_name="neo4j_acme",
        ))
        mock_driver = MagicMock()
        router = TenantRouter(registry)

        wrapper = router.get_connection("acme", mock_driver)
        wrapper.validate_query_tenant("acme")

        with pytest.raises(TenantIsolationViolation):
            wrapper.validate_query_tenant("evil_corp")

    def test_router_get_connection_unknown_tenant_raises(self) -> None:
        registry = TenantRegistry()
        mock_driver = MagicMock()
        router = TenantRouter(registry)

        with pytest.raises(LookupError):
            router.get_connection("unknown", mock_driver)


class TestOrphanedPoolDetection:

    def test_detects_orphaned_pool(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(tenant_id="active_corp"))

        pool_registry: Dict[str, Any] = {
            "active_corp": MagicMock(),
            "deleted_corp": MagicMock(),
        }

        detector = OrphanedPoolDetector(
            tenant_registry=registry,
            pool_registry=pool_registry,
        )
        orphaned = detector.check()

        assert "deleted_corp" in orphaned
        assert "active_corp" not in orphaned

    def test_no_orphans_when_all_active(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(tenant_id="a"))
        registry.register(TenantConfig(tenant_id="b"))

        pool_registry: Dict[str, Any] = {
            "a": MagicMock(),
            "b": MagicMock(),
        }

        detector = OrphanedPoolDetector(
            tenant_registry=registry,
            pool_registry=pool_registry,
        )
        orphaned = detector.check()

        assert orphaned == []

    def test_empty_pool_registry_returns_empty(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(tenant_id="a"))

        detector = OrphanedPoolDetector(
            tenant_registry=registry,
            pool_registry={},
        )
        orphaned = detector.check()

        assert orphaned == []

    def test_multiple_orphans_detected(self) -> None:
        registry = TenantRegistry()

        pool_registry: Dict[str, Any] = {
            "ghost_a": MagicMock(),
            "ghost_b": MagicMock(),
        }

        detector = OrphanedPoolDetector(
            tenant_registry=registry,
            pool_registry=pool_registry,
        )
        orphaned = detector.check()

        assert sorted(orphaned) == ["ghost_a", "ghost_b"]


class TestPhysicalIsolationBlocksCrossDatabaseQueries:

    def test_physical_mode_rejects_wrong_database(self) -> None:
        mock_driver = MagicMock()
        wrapper = TenantConnectionWrapper(
            driver=mock_driver,
            bound_tenant_id="acme",
            bound_database="neo4j_acme",
        )
        with pytest.raises(TenantIsolationViolation, match="database"):
            wrapper.validate_database("neo4j_evil")

    def test_physical_mode_allows_correct_database(self) -> None:
        mock_driver = MagicMock()
        wrapper = TenantConnectionWrapper(
            driver=mock_driver,
            bound_tenant_id="acme",
            bound_database="neo4j_acme",
        )
        wrapper.validate_database("neo4j_acme")


class TestValidateTenantBindingAtRequestStart:

    def test_mismatched_tenant_raises(self) -> None:
        mock_driver = MagicMock()
        wrapper = TenantConnectionWrapper(
            driver=mock_driver,
            bound_tenant_id="acme",
            bound_database="neo4j_acme",
        )
        with pytest.raises(TenantIsolationViolation):
            validate_tenant_binding(wrapper, "evil_corp")

    def test_matching_tenant_succeeds(self) -> None:
        mock_driver = MagicMock()
        wrapper = TenantConnectionWrapper(
            driver=mock_driver,
            bound_tenant_id="acme",
            bound_database="neo4j_acme",
        )
        validate_tenant_binding(wrapper, "acme")

    def test_matching_tenant_with_database_check(self) -> None:
        mock_driver = MagicMock()
        wrapper = TenantConnectionWrapper(
            driver=mock_driver,
            bound_tenant_id="acme",
            bound_database="neo4j_acme",
        )
        validate_tenant_binding(
            wrapper, "acme", expected_database="neo4j_acme",
        )

    def test_mismatched_database_raises(self) -> None:
        mock_driver = MagicMock()
        wrapper = TenantConnectionWrapper(
            driver=mock_driver,
            bound_tenant_id="acme",
            bound_database="neo4j_acme",
        )
        with pytest.raises(TenantIsolationViolation, match="database"):
            validate_tenant_binding(
                wrapper, "acme", expected_database="neo4j_other",
            )

    def test_non_wrapper_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="TenantConnectionWrapper"):
            validate_tenant_binding(MagicMock(), "acme")
