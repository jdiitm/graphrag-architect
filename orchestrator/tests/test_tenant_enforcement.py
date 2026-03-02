from __future__ import annotations

import pytest

from orchestrator.app.tenant_isolation import (
    IsolationMode,
    TenantEnforcingDriver,
    TenantIsolationViolation,
)


class TestTenantEnforcingDriver:

    def test_rejects_query_without_tenant_id_param_in_logical_mode(self) -> None:
        enforcer = TenantEnforcingDriver(
            isolation_mode=IsolationMode.LOGICAL,
        )
        with pytest.raises(TenantIsolationViolation, match="tenant_id"):
            enforcer.validate_query_params(
                query="MATCH (n:Service) RETURN n",
                params={"some_other_param": "value"},
            )

    def test_accepts_query_with_tenant_id_param_in_logical_mode(self) -> None:
        enforcer = TenantEnforcingDriver(
            isolation_mode=IsolationMode.LOGICAL,
        )
        enforcer.validate_query_params(
            query="MATCH (n:Service) WHERE n.tenant_id = $tenant_id RETURN n",
            params={"tenant_id": "t1"},
        )

    def test_skips_validation_in_physical_mode(self) -> None:
        enforcer = TenantEnforcingDriver(
            isolation_mode=IsolationMode.PHYSICAL,
        )
        enforcer.validate_query_params(
            query="MATCH (n:Service) RETURN n",
            params={},
        )

    def test_rejects_empty_tenant_id_value(self) -> None:
        enforcer = TenantEnforcingDriver(
            isolation_mode=IsolationMode.LOGICAL,
        )
        with pytest.raises(TenantIsolationViolation, match="tenant_id"):
            enforcer.validate_query_params(
                query="MATCH (n:Service) WHERE n.tenant_id = $tenant_id RETURN n",
                params={"tenant_id": ""},
            )

    def test_rejects_none_tenant_id_value(self) -> None:
        enforcer = TenantEnforcingDriver(
            isolation_mode=IsolationMode.LOGICAL,
        )
        with pytest.raises(TenantIsolationViolation, match="tenant_id"):
            enforcer.validate_query_params(
                query="MATCH (n:Service) WHERE n.tenant_id = $tenant_id RETURN n",
                params={"tenant_id": None},
            )

    def test_allows_schema_queries_without_tenant_id(self) -> None:
        enforcer = TenantEnforcingDriver(
            isolation_mode=IsolationMode.LOGICAL,
        )
        enforcer.validate_query_params(
            query="CREATE CONSTRAINT unique_service IF NOT EXISTS FOR (s:Service) REQUIRE s.id IS UNIQUE",
            params={},
        )

    def test_allows_dbms_queries_without_tenant_id(self) -> None:
        enforcer = TenantEnforcingDriver(
            isolation_mode=IsolationMode.LOGICAL,
        )
        enforcer.validate_query_params(
            query="CALL dbms.components() YIELD edition RETURN edition",
            params={},
        )
