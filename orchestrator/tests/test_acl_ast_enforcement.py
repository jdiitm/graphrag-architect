from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.tenant_security import (
    SecurityViolationError,
    TenantSecurityProvider,
    build_traversal_batched_neighbor,
    build_traversal_neighbor_discovery,
    build_traversal_one_hop,
    build_traversal_sampled_neighbor,
)


class TestRejectsMissingTenantId:
    def test_rejects_query_without_tenant_id_in_params(self):
        provider = TenantSecurityProvider()
        query = (
            "MATCH (n {id: $source_id, tenant_id: $tenant_id})-[r]->(t) "
            "WHERE t.tenant_id = $tenant_id "
            "AND ($is_admin OR t.team_owner = $acl_team "
            "OR ANY(ns IN t.namespace_acl WHERE ns IN $acl_namespaces)) "
            "RETURN t"
        )
        params: dict = {"source_id": "s1"}
        with pytest.raises(SecurityViolationError):
            provider.validate_query(query, params)

    def test_rejects_none_tenant_id(self):
        provider = TenantSecurityProvider()
        query = (
            "MATCH (n {id: $source_id, tenant_id: $tenant_id})-[r]->(t) "
            "WHERE t.tenant_id = $tenant_id "
            "AND ($is_admin OR t.team_owner = $acl_team "
            "OR ANY(ns IN t.namespace_acl WHERE ns IN $acl_namespaces)) "
            "RETURN t"
        )
        params: dict = {"source_id": "s1", "tenant_id": None}
        with pytest.raises(SecurityViolationError):
            provider.validate_query(query, params)

    def test_rejects_empty_string_tenant_id(self):
        provider = TenantSecurityProvider()
        query = (
            "MATCH (n {id: $source_id, tenant_id: $tenant_id})-[r]->(t) "
            "WHERE t.tenant_id = $tenant_id "
            "AND ($is_admin OR t.team_owner = $acl_team "
            "OR ANY(ns IN t.namespace_acl WHERE ns IN $acl_namespaces)) "
            "RETURN t"
        )
        params: dict = {"source_id": "s1", "tenant_id": ""}
        with pytest.raises(SecurityViolationError):
            provider.validate_query(query, params)


class TestRejectsQueryWithoutACLClause:
    def test_rejects_query_missing_acl_markers(self):
        provider = TenantSecurityProvider()
        query = "MATCH (n {id: $source_id, tenant_id: $tenant_id}) RETURN n"
        params = {"source_id": "s1", "tenant_id": "t1"}
        with pytest.raises(SecurityViolationError):
            provider.validate_query(query, params)

    def test_accepts_query_with_full_acl(self):
        provider = TenantSecurityProvider()
        query = (
            "MATCH (source {id: $source_id, tenant_id: $tenant_id})"
            "-[r]->(target) "
            "WHERE target.tenant_id = $tenant_id "
            "AND ($is_admin OR target.team_owner = $acl_team "
            "OR ANY(ns IN target.namespace_acl WHERE ns IN $acl_namespaces)) "
            "RETURN target"
        )
        params = {
            "source_id": "s1",
            "tenant_id": "t1",
            "is_admin": False,
            "acl_team": "platform",
            "acl_namespaces": ["prod"],
        }
        provider.validate_query(query, params)


class TestACLCannotBeOmittedFromBuilders:
    @pytest.mark.parametrize("rel_type", [
        "CALLS", "PRODUCES", "CONSUMES", "DEPLOYED_IN",
    ])
    def test_one_hop_always_has_acl(self, rel_type: str):
        cypher = build_traversal_one_hop(rel_type)
        assert "$tenant_id" in cypher
        assert "$is_admin" in cypher
        assert "$acl_team" in cypher
        assert "$acl_namespaces" in cypher
        assert "tenant_id" in cypher

    def test_neighbor_discovery_always_has_acl(self):
        cypher = build_traversal_neighbor_discovery()
        assert "$tenant_id" in cypher
        assert "$is_admin" in cypher
        assert "$acl_team" in cypher
        assert "$acl_namespaces" in cypher

    def test_sampled_neighbor_always_has_acl(self):
        cypher = build_traversal_sampled_neighbor()
        assert "$tenant_id" in cypher
        assert "$is_admin" in cypher
        assert "$acl_team" in cypher
        assert "$acl_namespaces" in cypher

    def test_batched_neighbor_always_has_acl(self):
        cypher = build_traversal_batched_neighbor()
        assert "$tenant_id" in cypher
        assert "$is_admin" in cypher
        assert "$acl_team" in cypher
        assert "$acl_namespaces" in cypher

    def test_all_builders_pass_provider_validation(self):
        provider = TenantSecurityProvider()
        acl_params: dict = {
            "source_id": "s1",
            "tenant_id": "t1",
            "is_admin": False,
            "acl_team": "platform",
            "acl_namespaces": ["prod"],
            "limit": 50,
            "sample_size": 50,
            "frontier_ids": ["s1"],
        }
        for builder_fn in [
            lambda: build_traversal_one_hop("CALLS"),
            build_traversal_neighbor_discovery,
            build_traversal_sampled_neighbor,
            build_traversal_batched_neighbor,
        ]:
            cypher = builder_fn()
            provider.validate_query(cypher, acl_params)

    def test_one_hop_rejects_disallowed_rel_type(self):
        with pytest.raises(ValueError, match="Disallowed"):
            build_traversal_one_hop("DELETE_ALL")


class TestASTQueriesMatchOldTemplates:
    @pytest.mark.parametrize("rel_type", [
        "CALLS", "PRODUCES", "CONSUMES", "DEPLOYED_IN",
    ])
    def test_one_hop_has_same_structure(self, rel_type: str):
        cypher = build_traversal_one_hop(rel_type)
        assert f"-[r:{rel_type}]->" in cypher
        assert "$source_id" in cypher
        assert "$tenant_id" in cypher
        assert "tombstoned_at IS NULL" in cypher
        assert "AS result" in cypher
        assert "type(r) AS rel_type" in cypher
        assert "$limit" in cypher

    def test_neighbor_discovery_has_same_structure(self):
        cypher = build_traversal_neighbor_discovery()
        assert "$source_id" in cypher
        assert "$tenant_id" in cypher
        assert "target.id AS target_id" in cypher
        assert "target.name AS target_name" in cypher
        assert "type(r) AS rel_type" in cypher
        assert "labels(target)[0] AS target_label" in cypher
        assert "$limit" in cypher

    def test_sampled_neighbor_has_same_structure(self):
        cypher = build_traversal_sampled_neighbor()
        assert "$source_id" in cypher
        assert "$tenant_id" in cypher
        assert "target.id AS target_id" in cypher
        assert "target.name AS target_name" in cypher
        assert "ORDER BY" in cypher.upper()
        assert "pagerank" in cypher
        assert "$sample_size" in cypher

    def test_batched_neighbor_has_same_structure(self):
        cypher = build_traversal_batched_neighbor()
        assert "UNWIND" in cypher
        assert "$frontier_ids" in cypher
        assert "$tenant_id" in cypher
        assert "source_id" in cypher
        assert "target_id" in cypher
        assert "$limit" in cypher


class TestTraversalExecutionGoesthroughProvider:
    @pytest.mark.asyncio
    async def test_execute_hop_calls_provider_validate(self):
        from orchestrator.app.agentic_traversal import execute_hop

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(side_effect=[
            [{"degree": 5}],
            [],
        ])
        mock_driver = MagicMock()

        @asynccontextmanager
        async def session_ctx(**kwargs):
            yield mock_session

        mock_driver.session = session_ctx

        with patch(
            "orchestrator.app.agentic_traversal.TenantSecurityProvider",
            create=True,
        ) as MockProvider:
            mock_instance = MagicMock()
            MockProvider.return_value = mock_instance

            await execute_hop(
                driver=mock_driver,
                source_id="s1",
                tenant_id="t1",
                acl_params={
                    "is_admin": False,
                    "acl_team": "platform",
                    "acl_namespaces": ["prod"],
                },
            )
            mock_instance.validate_query.assert_called()

    @pytest.mark.asyncio
    async def test_execute_batched_hop_calls_provider_validate(self):
        from orchestrator.app.agentic_traversal import execute_batched_hop

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(return_value=[])
        mock_driver = MagicMock()

        @asynccontextmanager
        async def session_ctx(**kwargs):
            yield mock_session

        mock_driver.session = session_ctx

        with patch(
            "orchestrator.app.agentic_traversal.TenantSecurityProvider",
            create=True,
        ) as MockProvider:
            mock_instance = MagicMock()
            MockProvider.return_value = mock_instance

            await execute_batched_hop(
                driver=mock_driver,
                source_ids=["s1", "s2"],
                tenant_id="t1",
                acl_params={
                    "is_admin": False,
                    "acl_team": "platform",
                    "acl_namespaces": ["prod"],
                },
            )
            mock_instance.validate_query.assert_called()
