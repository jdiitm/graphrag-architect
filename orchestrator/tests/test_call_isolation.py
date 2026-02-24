from __future__ import annotations

import pytest

from orchestrator.app.cypher_ast import (
    CallSubquery,
    CypherParser,
    MatchClause,
    inject_acl_all_scopes,
)
from orchestrator.app.call_isolation import (
    ASTVisitor,
    UnfilteredCallMatchError,
    validate_call_subquery_acl,
    count_acl_injection_depth,
)


class TestASTVisitor:
    def test_visits_top_level_match(self) -> None:
        ast = CypherParser("MATCH (n) RETURN n").parse()
        visited: list[str] = []
        visitor = ASTVisitor()
        visitor.on_match = lambda m, d: visited.append(f"match-{d}")
        visitor.walk(ast.clauses)
        assert len(visited) == 1
        assert visited[0] == "match-0"

    def test_visits_nested_call_match(self) -> None:
        ast = CypherParser(
            "CALL { MATCH (n) RETURN n } RETURN n"
        ).parse()
        visited: list[str] = []
        visitor = ASTVisitor()
        visitor.on_match = lambda m, d: visited.append(f"match-{d}")
        visitor.walk(ast.clauses)
        assert any("match-1" in v for v in visited)

    def test_visits_deeply_nested(self) -> None:
        ast = CypherParser(
            "CALL { CALL { MATCH (n) RETURN n } RETURN n } RETURN n"
        ).parse()
        visited: list[str] = []
        visitor = ASTVisitor()
        visitor.on_match = lambda m, d: visited.append(f"match-{d}")
        visitor.walk(ast.clauses)
        assert any("match-2" in v for v in visited)


class TestValidateCallSubqueryACL:
    def test_rejects_unfiltered_call_match(self) -> None:
        cypher = "CALL { MATCH (n:Service) RETURN n } RETURN n"
        with pytest.raises(UnfilteredCallMatchError):
            validate_call_subquery_acl(
                cypher,
                required_fields=("team_owner", "namespace_acl"),
            )

    def test_accepts_filtered_call_match(self) -> None:
        cypher = (
            "CALL { MATCH (n:Service) "
            "WHERE n.team_owner = $acl_team RETURN n } RETURN n"
        )
        validate_call_subquery_acl(
            cypher,
            required_fields=("team_owner",),
        )

    def test_accepts_no_call_subqueries(self) -> None:
        cypher = "MATCH (n) WHERE n.team_owner = 'x' RETURN n"
        validate_call_subquery_acl(
            cypher,
            required_fields=("team_owner",),
        )

    def test_injection_then_validation_passes(self) -> None:
        cypher = "CALL { MATCH (n:Service) RETURN n } RETURN n"
        injected = inject_acl_all_scopes(
            cypher, "n.team_owner = $acl_team",
        )
        validate_call_subquery_acl(
            injected,
            required_fields=("team_owner",),
        )


class TestACLInjectionDepth:
    def test_top_level_only(self) -> None:
        cypher = "MATCH (n) RETURN n"
        assert count_acl_injection_depth(cypher) == 0

    def test_single_call_depth(self) -> None:
        cypher = "CALL { MATCH (n) RETURN n } RETURN n"
        assert count_acl_injection_depth(cypher) == 1

    def test_double_nested(self) -> None:
        cypher = (
            "CALL { CALL { MATCH (n) RETURN n } RETURN n } RETURN n"
        )
        assert count_acl_injection_depth(cypher) == 2
