from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.tenant_isolation import (
    IsolationMode,
    TenantConfig,
    TenantIsolationViolation,
    TenantRegistry,
)


def _mock_driver_with_edition(edition: str) -> MagicMock:
    mock_record = {"name": "Neo4j Kernel", "version": "5.26.0", "edition": edition}
    mock_result = AsyncMock()
    mock_result.single = AsyncMock(return_value=mock_record)

    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=mock_result)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    mock_driver = MagicMock()
    mock_driver.session = MagicMock(return_value=mock_session)
    return mock_driver


class TestDetectNeo4jEdition:

    @pytest.mark.asyncio
    async def test_detects_community_edition(self) -> None:
        from orchestrator.app.tenant_isolation import detect_neo4j_edition

        driver = _mock_driver_with_edition("community")
        edition = await detect_neo4j_edition(driver)
        assert edition == "community"

    @pytest.mark.asyncio
    async def test_detects_enterprise_edition(self) -> None:
        from orchestrator.app.tenant_isolation import detect_neo4j_edition

        driver = _mock_driver_with_edition("enterprise")
        edition = await detect_neo4j_edition(driver)
        assert edition == "enterprise"


class TestValidatePhysicalIsolationSupport:

    @pytest.mark.asyncio
    async def test_community_with_physical_raises(self) -> None:
        from orchestrator.app.tenant_isolation import (
            validate_physical_isolation_support,
        )

        driver = _mock_driver_with_edition("community")

        registry = TenantRegistry()
        registry.register(TenantConfig(
            tenant_id="acme",
            isolation_mode=IsolationMode.PHYSICAL,
            database_name="acme_db",
        ))

        with pytest.raises(TenantIsolationViolation, match="[Cc]ommunity"):
            await validate_physical_isolation_support(driver, registry)

    @pytest.mark.asyncio
    async def test_enterprise_with_physical_passes(self) -> None:
        from orchestrator.app.tenant_isolation import (
            validate_physical_isolation_support,
        )

        driver = _mock_driver_with_edition("enterprise")

        registry = TenantRegistry()
        registry.register(TenantConfig(
            tenant_id="acme",
            isolation_mode=IsolationMode.PHYSICAL,
            database_name="acme_db",
        ))

        await validate_physical_isolation_support(driver, registry)

    @pytest.mark.asyncio
    async def test_community_with_logical_only_passes(self) -> None:
        from orchestrator.app.tenant_isolation import (
            validate_physical_isolation_support,
        )

        driver = _mock_driver_with_edition("community")

        registry = TenantRegistry()
        registry.register(TenantConfig(
            tenant_id="acme",
            isolation_mode=IsolationMode.LOGICAL,
            database_name="neo4j",
        ))

        await validate_physical_isolation_support(driver, registry)

    @pytest.mark.asyncio
    async def test_empty_registry_passes(self) -> None:
        from orchestrator.app.tenant_isolation import (
            validate_physical_isolation_support,
        )

        driver = _mock_driver_with_edition("community")
        registry = TenantRegistry()

        await validate_physical_isolation_support(driver, registry)
