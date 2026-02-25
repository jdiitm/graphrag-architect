from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from orchestrator.app.tenant_isolation import IsolationMode, TenantConfig, TenantContext


class TestPhysicalIsolationDefault:

    def test_tenant_config_defaults_to_physical(self) -> None:
        config = TenantConfig(tenant_id="acme")
        assert config.isolation_mode == IsolationMode.PHYSICAL, (
            "TenantConfig must default to PHYSICAL isolation for production safety. "
            f"Got: {config.isolation_mode}"
        )

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
