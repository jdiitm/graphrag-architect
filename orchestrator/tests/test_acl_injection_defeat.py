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
    def test_call_subquery_acl_not_injected_inside_braces(self):
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
        acl_pos = filtered.find("n.team_owner")
        assert acl_pos > brace_end, "ACL must be outside the subquery braces"

    def test_deeply_nested_call_subqueries(self):
        filt = _viewer_filter()
        cypher = (
            "MATCH (n:Service) "
            "CALL { CALL { MATCH (m) RETURN m } RETURN m } "
            "RETURN n"
        )
        filtered, params = filt.inject_into_cypher(cypher)
        assert params["acl_team"] == "platform"
        last_brace = filtered.rfind("}")
        acl_pos = filtered.find("n.team_owner")
        assert acl_pos > last_brace


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


import pytest
