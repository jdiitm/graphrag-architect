from __future__ import annotations

import pathlib
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.tenant_query_guard import (
    SCHEMA_DDL_ALLOWLIST,
    CypherTenantGuard,
    TenantScopedSession,
    TenantScopeViolationError,
)


class TestTenantScopedSessionRejectsUnscopedQuery:

    def test_rejects_query_without_tenant_id_and_not_allowlisted(self) -> None:
        session = TenantScopedSession(tenant_id="acme-corp")
        with pytest.raises(TenantScopeViolationError, match="tenant_id"):
            session.validate_query("MATCH (n:Service) RETURN n", {})

    def test_allows_query_containing_tenant_id_parameter(self) -> None:
        session = TenantScopedSession(tenant_id="acme-corp")
        validated_params = session.validate_query(
            "MATCH (n:Service {tenant_id: $tenant_id}) RETURN n",
            {"tenant_id": "acme-corp"},
        )
        assert validated_params["tenant_id"] == "acme-corp"

    def test_injects_tenant_id_when_query_references_it_but_params_omit_it(self) -> None:
        session = TenantScopedSession(tenant_id="acme-corp")
        validated_params = session.validate_query(
            "MATCH (n {tenant_id: $tenant_id}) RETURN n",
            {},
        )
        assert validated_params["tenant_id"] == "acme-corp"

    def test_raises_when_param_tenant_id_mismatches_session(self) -> None:
        session = TenantScopedSession(tenant_id="acme-corp")
        with pytest.raises(TenantScopeViolationError, match="mismatch"):
            session.validate_query(
                "MATCH (n {tenant_id: $tenant_id}) RETURN n",
                {"tenant_id": "evil-corp"},
            )

    def test_allows_allowlisted_schema_ddl_without_tenant_id(self) -> None:
        session = TenantScopedSession(tenant_id="acme-corp")
        for ddl_pattern in SCHEMA_DDL_ALLOWLIST:
            validated = session.validate_query(ddl_pattern, {})
            assert "tenant_id" not in validated or validated.get("tenant_id") == "acme-corp"

    def test_allows_custom_allowlist_entry(self) -> None:
        custom_allowlist = frozenset({"CALL dbms.components() YIELD edition RETURN edition"})
        session = TenantScopedSession(
            tenant_id="acme-corp",
            allowlist=custom_allowlist,
        )
        validated = session.validate_query(
            "CALL dbms.components() YIELD edition RETURN edition", {},
        )
        assert isinstance(validated, dict)


class TestReadTopologyRequiresTenantId:

    @pytest.mark.asyncio
    async def test_read_topology_requires_tenant_id(self) -> None:
        from orchestrator.app.neo4j_client import GraphRepository

        mock_driver = MagicMock()
        repo = GraphRepository(driver=mock_driver)

        with pytest.raises(TypeError):
            await repo.read_topology(label="Service")

    @pytest.mark.asyncio
    async def test_read_topology_includes_tenant_id_in_cypher(self) -> None:
        from orchestrator.app.neo4j_client import GraphRepository

        executed_queries: list[str] = []

        async def _capture_tx(tx_func, **kwargs):
            mock_tx = AsyncMock()
            mock_result = AsyncMock()
            mock_result.data = AsyncMock(return_value=[])

            async def _capture_run(query, **params):
                executed_queries.append(query)
                return mock_result

            mock_tx.run = _capture_run
            return await tx_func(mock_tx)

        mock_session = AsyncMock()
        mock_session.execute_read = _capture_tx
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = MagicMock()
        mock_driver.session.return_value = mock_session

        repo = GraphRepository(driver=mock_driver)
        await repo.read_topology(label="Service", tenant_id="acme-corp")

        assert len(executed_queries) == 1
        assert "tenant_id" in executed_queries[0]


class TestRefreshDegreePropertyRequiresTenantId:

    @pytest.mark.asyncio
    async def test_refresh_degree_requires_tenant_id(self) -> None:
        from orchestrator.app.neo4j_client import GraphRepository

        mock_driver = MagicMock()
        repo = GraphRepository(driver=mock_driver)

        with pytest.raises(TypeError):
            await repo._refresh_degree_property(["node-1"])

    @pytest.mark.asyncio
    async def test_refresh_degree_includes_tenant_in_cypher(self) -> None:
        from orchestrator.app.neo4j_client import GraphRepository

        executed_queries: list[str] = []

        async def _capture_tx(tx_func, **kwargs):
            mock_tx = AsyncMock()

            async def _capture_run(query, **params):
                executed_queries.append(query)
                mock_result = AsyncMock()
                return mock_result

            mock_tx.run = _capture_run
            return await tx_func(mock_tx)

        mock_session = AsyncMock()
        mock_session.execute_write = _capture_tx
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = MagicMock()
        mock_driver.session.return_value = mock_session

        repo = GraphRepository(driver=mock_driver)
        await repo._refresh_degree_property(["node-1"], tenant_id="acme-corp")

        assert len(executed_queries) == 1
        assert "tenant_id" in executed_queries[0]


class TestCypherTenantGuard:

    def test_catches_deliberately_unscoped_constant(self) -> None:
        unscoped_queries = {"MATCH (n:Service) RETURN n"}
        guard = CypherTenantGuard(allowlist=SCHEMA_DDL_ALLOWLIST)
        violations = guard.scan_queries(unscoped_queries)
        assert len(violations) >= 1
        assert "MATCH (n:Service) RETURN n" in violations

    def test_passes_scoped_constant(self) -> None:
        scoped_queries = {
            "MATCH (n:Service {tenant_id: $tenant_id}) RETURN n",
        }
        guard = CypherTenantGuard(allowlist=SCHEMA_DDL_ALLOWLIST)
        violations = guard.scan_queries(scoped_queries)
        assert len(violations) == 0

    def test_passes_allowlisted_ddl(self) -> None:
        ddl_queries = set(SCHEMA_DDL_ALLOWLIST)
        guard = CypherTenantGuard(allowlist=SCHEMA_DDL_ALLOWLIST)
        violations = guard.scan_queries(ddl_queries)
        assert len(violations) == 0

    def test_all_existing_cypher_constants_are_scoped(self) -> None:
        guard = CypherTenantGuard(allowlist=SCHEMA_DDL_ALLOWLIST)
        app_dir = pathlib.Path(__file__).parent.parent / "app"
        all_cypher = guard.extract_cypher_constants_from_directory(app_dir)
        violations = guard.scan_queries(all_cypher)
        assert violations == set(), (
            f"Found {len(violations)} unscoped Cypher queries: "
            f"{violations}"
        )
