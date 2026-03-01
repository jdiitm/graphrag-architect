from __future__ import annotations

import re

import pytest

from orchestrator.app.cypher_ast import inject_limit_ast
from orchestrator.app.cypher_sandbox import SandboxedQueryExecutor, CypherSandboxConfig


class TestASTLimitInjectionBasics:

    def test_function_exists(self) -> None:
        assert callable(inject_limit_ast)

    def test_appends_limit_when_missing(self) -> None:
        result = inject_limit_ast("MATCH (n:Service) RETURN n", 1000)
        assert "LIMIT" in result.upper()
        limits = re.findall(r"\bLIMIT\s+(\d+)", result, re.IGNORECASE)
        assert len(limits) == 1
        assert int(limits[0]) == 1000

    def test_preserves_existing_limit_within_bounds(self) -> None:
        result = inject_limit_ast("MATCH (n:Service) RETURN n LIMIT 50", 1000)
        limits = re.findall(r"\bLIMIT\s+(\d+)", result, re.IGNORECASE)
        assert len(limits) == 1
        assert int(limits[0]) == 50

    def test_caps_excessive_limit(self) -> None:
        result = inject_limit_ast("MATCH (n:Service) RETURN n LIMIT 999999", 1000)
        limits = re.findall(r"\bLIMIT\s+(\d+)", result, re.IGNORECASE)
        assert len(limits) >= 1
        assert int(limits[0]) <= 1000


class TestASTLimitSubquerySemantics:

    def test_subquery_limit_capped_independently(self) -> None:
        cypher = (
            "MATCH (n:Service) "
            "CALL { MATCH (m:Database) RETURN m LIMIT 50000 } "
            "RETURN n, m LIMIT 100"
        )
        result = inject_limit_ast(cypher, 200)
        limits = re.findall(r"\bLIMIT\s+(\d+)", result, re.IGNORECASE)
        for val in limits:
            assert int(val) <= 200

    def test_string_literal_limit_not_modified(self) -> None:
        cypher = (
            'MATCH (n:Service) '
            'WHERE n.description = "LIMIT 9999" '
            'RETURN n'
        )
        result = inject_limit_ast(cypher, 100)
        assert "LIMIT 9999" in result, (
            "LIMIT inside a string literal must not be modified by AST injection. "
            f"Got: {result}"
        )
        from orchestrator.app.cypher_tokenizer import tokenize_cypher, TokenType
        tokens = tokenize_cypher(result)
        keyword_limits = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD and t.value.upper() == "LIMIT"
        ]
        assert len(keyword_limits) == 1, (
            "Exactly one keyword LIMIT should exist (the appended safety cap). "
            f"Got {len(keyword_limits)} keyword LIMITs in: {result}"
        )

    def test_union_branches_each_get_limit_cap(self) -> None:
        cypher = (
            "MATCH (a:Service) RETURN a LIMIT 5000 "
            "UNION "
            "MATCH (b:Database) RETURN b LIMIT 5000"
        )
        result = inject_limit_ast(cypher, 200)
        limits = re.findall(r"\bLIMIT\s+(\d+)", result, re.IGNORECASE)
        for val in limits:
            assert int(val) <= 200


class TestASTLimitEdgeCases:

    def test_with_clause_limit_capped(self) -> None:
        cypher = (
            "MATCH (n:Service) "
            "WITH n LIMIT 5000 "
            "MATCH (n)-[:CALLS]->(m) "
            "RETURN m"
        )
        result = inject_limit_ast(cypher, 200)
        limits = re.findall(r"\bLIMIT\s+(\d+)", result, re.IGNORECASE)
        for val in limits:
            assert int(val) <= 200
        assert any(int(v) == 200 for v in limits), (
            "A LIMIT must be appended or capped to max_results"
        )

    def test_query_with_skip_and_limit(self) -> None:
        cypher = "MATCH (n:Service) RETURN n SKIP 10 LIMIT 500"
        result = inject_limit_ast(cypher, 100)
        limits = re.findall(r"\bLIMIT\s+(\d+)", result, re.IGNORECASE)
        assert len(limits) >= 1
        assert int(limits[0]) <= 100
        assert "SKIP" in result.upper()

    def test_empty_query_returns_with_limit(self) -> None:
        result = inject_limit_ast("RETURN 1", 100)
        assert "LIMIT" in result.upper()


class TestSandboxUsesAST:

    def test_executor_inject_limit_uses_ast_not_regex(self) -> None:
        config = CypherSandboxConfig(max_results=100)
        executor = SandboxedQueryExecutor(config)
        cypher = (
            'MATCH (n:Service) '
            'WHERE n.description = "LIMIT 9999" '
            'RETURN n'
        )
        result = executor.inject_limit(cypher)
        assert "LIMIT 9999" in result, (
            "inject_limit must use AST-based approach that preserves "
            "string literal content. Regex-based approach would modify it. "
            f"Got: {result}"
        )
