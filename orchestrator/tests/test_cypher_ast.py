from __future__ import annotations

import pytest

from orchestrator.app.cypher_ast import (
    CallSubquery,
    CypherParser,
    MatchClause,
    ReturnClause,
    UnionQuery,
    WhereClause,
    WithClause,
    inject_acl_all_scopes,
    reconstruct_from_ast,
)


class TestCypherParser:
    def test_simple_match_return(self) -> None:
        ast = CypherParser("MATCH (n) RETURN n").parse()
        assert len(ast.clauses) >= 2
        assert any(isinstance(c, MatchClause) for c in ast.clauses)
        assert any(isinstance(c, ReturnClause) for c in ast.clauses)

    def test_match_where_return(self) -> None:
        ast = CypherParser(
            "MATCH (n) WHERE n.name = 'auth' RETURN n"
        ).parse()
        where_clauses = [c for c in ast.clauses if isinstance(c, WhereClause)]
        assert len(where_clauses) == 1

    def test_with_clause(self) -> None:
        ast = CypherParser(
            "MATCH (n) WITH n MATCH (n)-[r]-(m) RETURN m"
        ).parse()
        with_clauses = [c for c in ast.clauses if isinstance(c, WithClause)]
        assert len(with_clauses) == 1

    def test_call_subquery(self) -> None:
        ast = CypherParser(
            "CALL { MATCH (n) RETURN n } RETURN n"
        ).parse()
        call_clauses = [c for c in ast.clauses if isinstance(c, CallSubquery)]
        assert len(call_clauses) == 1
        inner = call_clauses[0]
        assert any(isinstance(c, MatchClause) for c in inner.body)

    def test_union_query(self) -> None:
        ast = CypherParser(
            "MATCH (n:Service) RETURN n UNION MATCH (m:Database) RETURN m"
        ).parse()
        union_parts = [c for c in ast.clauses if isinstance(c, UnionQuery)]
        assert len(union_parts) >= 1

    def test_round_trip_fidelity(self) -> None:
        original = "MATCH (n:Service) WHERE n.name = 'auth' RETURN n.name"
        ast = CypherParser(original).parse()
        reconstructed = reconstruct_from_ast(ast)
        normalized_orig = " ".join(original.split())
        normalized_recon = " ".join(reconstructed.split())
        assert normalized_orig == normalized_recon


class TestACLInjection:
    def test_inject_into_simple_match(self) -> None:
        cypher = "MATCH (n:Service) RETURN n"
        result = inject_acl_all_scopes(
            cypher, "n.team_owner = $acl_team",
        )
        assert "$acl_team" in result
        assert "WHERE" in result.upper()

    def test_inject_into_existing_where(self) -> None:
        cypher = "MATCH (n) WHERE n.name = 'auth' RETURN n"
        result = inject_acl_all_scopes(
            cypher, "n.team_owner = $acl_team",
        )
        assert "$acl_team" in result
        assert "AND" in result.upper()

    def test_inject_into_call_subquery(self) -> None:
        cypher = "CALL { MATCH (n:Service) RETURN n } RETURN n"
        result = inject_acl_all_scopes(
            cypher, "n.team_owner = $acl_team",
        )
        assert result.count("$acl_team") >= 1

    def test_inject_into_union_branches(self) -> None:
        cypher = (
            "MATCH (n:Service) RETURN n "
            "UNION "
            "MATCH (m:Database) RETURN m"
        )
        result = inject_acl_all_scopes(
            cypher, "n.team_owner = $acl_team",
        )
        assert result.upper().count("$ACL_TEAM") >= 2

    def test_no_injection_without_match(self) -> None:
        cypher = "RETURN 1 AS value"
        result = inject_acl_all_scopes(
            cypher, "n.team_owner = $acl_team",
        )
        assert "$acl_team" not in result

    def test_preserves_string_literals(self) -> None:
        cypher = "MATCH (n) WHERE n.name = 'MATCH WHERE' RETURN n"
        result = inject_acl_all_scopes(
            cypher, "n.team_owner = $acl_team",
        )
        assert "'MATCH WHERE'" in result
