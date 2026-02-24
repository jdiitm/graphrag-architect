import pytest

from orchestrator.app.access_control import CypherPermissionFilter, SecurityPrincipal


def _viewer_filter() -> CypherPermissionFilter:
    return CypherPermissionFilter(
        SecurityPrincipal(team="platform", namespace="production", role="viewer")
    )


class TestACLInjectionUnionBypass:
    def test_union_both_clauses_get_acl(self):
        filt = _viewer_filter()
        cypher = (
            "MATCH (n:Service) RETURN n "
            "UNION "
            "MATCH (n:Service) RETURN n"
        )
        filtered, params = filt.inject_into_cypher(cypher)
        assert params["acl_team"] == "platform"
        acl_count = filtered.count("n.team_owner")
        assert acl_count >= 2, (
            f"Both UNION clauses must have ACL but found {acl_count} occurrences"
        )

    def test_union_all_both_clauses_get_acl(self):
        filt = _viewer_filter()
        cypher = (
            "MATCH (n:Service) RETURN n "
            "UNION ALL "
            "MATCH (n:Service) RETURN n"
        )
        filtered, params = filt.inject_into_cypher(cypher)
        acl_count = filtered.count("n.team_owner")
        assert acl_count >= 2


class TestACLInjectionWithClause:
    def test_with_clause_acl_applied_after_final_match(self):
        filt = _viewer_filter()
        cypher = "MATCH (n:Service) WITH n WHERE n.active = true RETURN n"
        filtered, params = filt.inject_into_cypher(cypher)
        assert params["acl_team"] == "platform"
        assert "n.team_owner" in filtered
        assert "n.active = true" in filtered

    def test_with_realiasing_uses_correct_alias(self):
        filt = _viewer_filter()
        cypher = "MATCH (n:Service) WITH n AS svc RETURN svc"
        filtered, params = filt.inject_into_cypher(cypher, alias="n")
        assert params["acl_team"] == "platform"
        assert "n.team_owner" in filtered


class TestACLInjectionCommentBypass:
    def test_single_line_comment_cannot_hide_return(self):
        filt = _viewer_filter()
        cypher = "MATCH (n:Service) // RETURN n\nRETURN n"
        filtered, params = filt.inject_into_cypher(cypher)
        assert params["acl_team"] == "platform"
        assert "n.team_owner" in filtered

    def test_block_comment_cannot_hide_where(self):
        filt = _viewer_filter()
        cypher = "MATCH (n:Service) /* WHERE n.team = 'evil' */ RETURN n"
        filtered, params = filt.inject_into_cypher(cypher)
        assert params["acl_team"] == "platform"
        assert "n.team_owner" in filtered


class TestACLInjectionNestedSubquery:
    def test_call_subquery_acl_injected_inside_braces(self):
        filt = _viewer_filter()
        cypher = (
            "MATCH (n:Service) "
            "CALL { WITH n MATCH (n)-[:CALLS]->(t) RETURN t } "
            "RETURN n, t"
        )
        filtered, params = filt.inject_into_cypher(cypher)
        assert params["acl_team"] == "platform"
        brace_start = filtered.find("{")
        brace_end = filtered.find("}")
        inner_acl = filtered.find("n.team_owner", brace_start)
        assert inner_acl < brace_end, (
            "AST-level ACL must inject inside CALL subquery MATCH clauses"
        )

    def test_deeply_nested_call_subqueries_covered(self):
        filt = _viewer_filter()
        cypher = (
            "MATCH (n:Service) "
            "CALL { CALL { MATCH (m) RETURN m } RETURN m } "
            "RETURN n"
        )
        filtered, params = filt.inject_into_cypher(cypher)
        assert params["acl_team"] == "platform"
        inner_brace = filtered.find("{", filtered.find("{") + 1)
        inner_brace_end = filtered.find("}")
        inner_acl = filtered.find("n.team_owner", inner_brace)
        assert inner_acl < inner_brace_end, (
            "AST-level ACL must inject into deeply nested MATCH clauses"
        )


class TestACLInjectionMultiStatement:
    def test_semicolon_separated_rejected_by_validator(self):
        from orchestrator.app.cypher_validator import (
            CypherValidationError,
            validate_cypher_readonly,
        )
        cypher = "MATCH (n) RETURN n; MATCH (m) RETURN m"
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly(cypher)


class TestACLInjectionStringLiteralBypass:
    def test_where_inside_string_not_treated_as_keyword(self):
        filt = _viewer_filter()
        cypher = "MATCH (n:Service) WHERE n.name = 'WHERE_SVC' RETURN n"
        filtered, params = filt.inject_into_cypher(cypher)
        assert "'WHERE_SVC'" in filtered
        assert params["acl_team"] == "platform"

    def test_return_inside_string_not_treated_as_keyword(self):
        filt = _viewer_filter()
        cypher = "MATCH (n:Service) WHERE n.desc CONTAINS 'RETURN value' RETURN n"
        filtered, params = filt.inject_into_cypher(cypher)
        assert "'RETURN value'" in filtered
        assert params["acl_team"] == "platform"
        final_return_pos = filtered.rfind("RETURN n")
        acl_pos = filtered.find("n.team_owner")
        assert acl_pos < final_return_pos


class TestDefaultDenyUntagged:
    def test_deny_untagged_excludes_null_team_owner(self):
        filt = CypherPermissionFilter(
            SecurityPrincipal(
                team="platform", namespace="production", role="viewer",
            ),
            default_deny_untagged=True,
        )
        clause, _ = filt.node_filter("n")
        assert "IS NULL" not in clause

    def test_deny_untagged_excludes_null_namespace_acl(self):
        filt = CypherPermissionFilter(
            SecurityPrincipal(
                team="platform", namespace="production", role="viewer",
            ),
            default_deny_untagged=True,
        )
        clause, _ = filt.node_filter("n")
        assert "IS NULL" not in clause

    def test_allow_untagged_includes_null_fallback(self):
        filt = CypherPermissionFilter(
            SecurityPrincipal(
                team="platform", namespace="production", role="viewer",
            ),
            default_deny_untagged=False,
        )
        clause, _ = filt.node_filter("n")
        assert "IS NULL" in clause

    def test_default_deny_is_true(self):
        filt = CypherPermissionFilter(
            SecurityPrincipal(
                team="platform", namespace="production", role="viewer",
            ),
        )
        clause, _ = filt.node_filter("n")
        assert "IS NULL" not in clause

    def test_anonymous_with_deny_gets_strict_public_filter(self):
        filt = CypherPermissionFilter(
            SecurityPrincipal(team="*", namespace="*", role="anonymous"),
            default_deny_untagged=True,
        )
        clause, params = filt.node_filter("n")
        assert params["acl_team"] == "public"
        assert "IS NULL" not in clause

    def test_anonymous_with_allow_gets_null_fallback(self):
        filt = CypherPermissionFilter(
            SecurityPrincipal(team="*", namespace="*", role="anonymous"),
            default_deny_untagged=False,
        )
        clause, params = filt.node_filter("n")
        assert params["acl_team"] == "public"
        assert "IS NULL" in clause
