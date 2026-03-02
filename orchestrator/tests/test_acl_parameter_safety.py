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
        assert "$acl_labels" in fragment
        assert "labels(" in fragment
        non_param_names = re.findall(r"(?<!\$)\bacl_labels\b", fragment)
        assert len(non_param_names) == 0, (
            "ACL fragment must use only $-prefixed parameters, never literals; "
            f"found bare references: {non_param_names}"
        )

    def test_traversal_templates_use_dollar_params(self) -> None:
        cypher = build_traversal_neighbor_discovery()
        assert "$tenant_id" in cypher
        assert "$is_admin" in cypher
        assert "$acl_labels" in cypher
        assert "labels(" in cypher
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
        assert frag1.count("$") >= 2, (
            "ACL fragment must have at least 2 parameter placeholders "
            "($is_admin, $acl_labels)"
        )
