from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.query_templates import (
    ALLOWED_RELATIONSHIP_TYPES,
    TemplateCatalog,
    build_acl_multi_hop_query,
    build_acl_params,
    build_acl_single_hop_query,
    dynamic_cypher_allowed,
)


class TestDynamicCypherGate:
    def test_defaults_to_false(self):
        with patch.dict("os.environ", {}, clear=True):
            assert dynamic_cypher_allowed() is False

    def test_enabled_by_env_true(self):
        with patch.dict("os.environ", {"ALLOW_DYNAMIC_CYPHER": "true"}):
            assert dynamic_cypher_allowed() is True

    def test_disabled_by_env_false(self):
        with patch.dict("os.environ", {"ALLOW_DYNAMIC_CYPHER": "false"}):
            assert dynamic_cypher_allowed() is False


class TestBuildAclSingleHopQuery:
    def test_allowed_relationship_type_produces_cypher(self):
        for rel_type in ALLOWED_RELATIONSHIP_TYPES:
            cypher = build_acl_single_hop_query(rel_type)
            assert f"-[r:{rel_type}]->" in cypher
            assert "$tenant_id" in cypher
            assert "$is_admin" in cypher
            assert "$acl_team" in cypher
            assert "$acl_namespaces" in cypher
            assert "$limit" in cypher

    def test_disallowed_relationship_type_raises(self):
        with pytest.raises(ValueError, match="Disallowed"):
            build_acl_single_hop_query("DELETE_ALL")

    def test_no_string_interpolation_of_user_values(self):
        cypher = build_acl_single_hop_query("CALLS")
        assert "$source_id" in cypher
        assert "$tenant_id" in cypher


class TestBuildAclMultiHopQuery:
    def test_respects_max_depth(self):
        cypher = build_acl_multi_hop_query(3)
        assert "*1..3" in cypher
        assert "$tenant_id" in cypher
        assert "$is_admin" in cypher

    def test_caps_depth_at_five(self):
        cypher = build_acl_multi_hop_query(100)
        assert "*1..5" in cypher

    def test_acl_applied_to_all_path_nodes(self):
        cypher = build_acl_multi_hop_query(3)
        assert "ALL(n IN nodes(path)" in cypher
        assert "$acl_team" in cypher
        assert "$acl_namespaces" in cypher

    def test_tombstone_filter_on_relationships(self):
        cypher = build_acl_multi_hop_query(3)
        assert "tombstoned_at IS NULL" in cypher


class TestBuildAclParams:
    def test_produces_correct_keys(self):
        params = build_acl_params(
            tenant_id="t1",
            is_admin=False,
            team="platform",
            namespaces=["prod", "staging"],
        )
        assert params["tenant_id"] == "t1"
        assert params["is_admin"] is False
        assert params["acl_team"] == "platform"
        assert params["acl_namespaces"] == ["prod", "staging"]

    def test_admin_flag_propagated(self):
        params = build_acl_params(
            tenant_id="t1",
            is_admin=True,
            team="ops",
            namespaces=[],
        )
        assert params["is_admin"] is True


class TestAllTemplatesHaveTenantId:
    def test_every_template_requires_tenant_id(self):
        catalog = TemplateCatalog()
        for name, template in catalog.all_templates().items():
            assert "$tenant_id" in template.cypher, (
                f"Template '{name}' must include $tenant_id parameter"
            )

    def test_acl_templates_have_full_acl_params(self):
        catalog = TemplateCatalog()
        for name in ("acl_vector_search", "acl_fulltext_search"):
            template = catalog.get_acl_template(name)
            assert template is not None, f"ACL template {name} must exist"
            assert "$tenant_id" in template.cypher
            assert "$is_admin" in template.cypher
            assert "$acl_team" in template.cypher
            assert "$acl_namespaces" in template.cypher


class TestQueryEngineUsesParameterizedACL:
    @pytest.mark.asyncio
    async def test_try_template_match_passes_acl_params_not_injection(self):
        from orchestrator.app.query_engine import _try_template_match

        mock_driver = AsyncMock()
        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(return_value=[{"name": "auth"}])
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver.session = MagicMock(return_value=mock_session)

        state = {
            "query": "What is the blast radius if auth-service fails?",
            "tenant_id": "t1",
            "authorization": "",
        }

        with patch(
            "orchestrator.app.query_engine._apply_acl"
        ) as mock_apply_acl, patch(
            "orchestrator.app.query_engine._execute_sandboxed_read",
            new_callable=AsyncMock,
            return_value=[{"name": "auth"}],
        ):
            mock_apply_acl.side_effect = AssertionError(
                "_apply_acl should not be called when template uses "
                "parameterized ACL"
            )
            result = await _try_template_match(state, mock_driver)
            assert result is not None
