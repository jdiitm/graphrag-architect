from __future__ import annotations

import re

import pytest

from orchestrator.app.tenant_security import (
    TenantSecurityProvider,
    _acl_where_fragment,
    build_traversal_one_hop,
    build_traversal_neighbor_discovery,
    build_traversal_batched_neighbor,
)


class TestACLFragmentParameterSafety:

    def test_acl_fragment_uses_only_dollar_params(self) -> None:
        fragment = _acl_where_fragment()
        assert "$is_admin" in fragment
        assert "$acl_team" in fragment
        assert "$acl_namespaces" in fragment
        non_param_names = re.findall(r"(?<!\$)\bacl_team\b", fragment)
        assert all(
            "$" + name.replace("acl_team", "acl_team") for name in non_param_names
        ), (
            "ACL fragment must use only $-prefixed parameters, never literals"
        )

    def test_traversal_templates_use_dollar_params(self) -> None:
        cypher = build_traversal_neighbor_discovery()
        assert "$tenant_id" in cypher
        assert "$is_admin" in cypher
        assert "$acl_team" in cypher
        assert "$acl_namespaces" in cypher
        assert "$source_id" in cypher
        assert "$limit" in cypher

    def test_batched_traversal_uses_dollar_params(self) -> None:
        cypher = build_traversal_batched_neighbor()
        assert "$frontier_ids" in cypher
        assert "$tenant_id" in cypher
        assert "$is_admin" in cypher
        assert "$limit" in cypher

    def test_one_hop_traversal_uses_dollar_params(self) -> None:
        cypher = build_traversal_one_hop("CALLS")
        assert "$source_id" in cypher
        assert "$tenant_id" in cypher
        assert "$is_admin" in cypher
        assert "$limit" in cypher

    def test_no_fstring_user_values_in_cypher(self) -> None:
        cypher = build_traversal_one_hop("CALLS")
        user_dangerous_patterns = [
            "f'{", 'f"{', "format(",
        ]
        for pattern in user_dangerous_patterns:
            assert pattern not in cypher, (
                f"Cypher template must not contain {pattern!r}"
            )

    def test_acl_fragment_is_static_string(self) -> None:
        frag1 = _acl_where_fragment()
        frag2 = _acl_where_fragment()
        assert frag1 == frag2, "ACL fragment must be a static template"
        assert frag1.count("$") >= 3, (
            "ACL fragment must have at least 3 parameter placeholders"
        )
