from __future__ import annotations

import pytest

from orchestrator.app.cypher_ast import CypherParser
from orchestrator.app.cypher_tokenizer import tokenize_cypher


class TestCypherInputLengthCap:
    def test_parser_rejects_oversized_input(self) -> None:
        oversized = "MATCH (n) RETURN n " + "x" * (65 * 1024)
        with pytest.raises(ValueError, match="[Ll]ength|[Ss]ize"):
            CypherParser(oversized)

    def test_tokenizer_rejects_oversized_input(self) -> None:
        oversized = "MATCH (n) RETURN n " + "x" * (65 * 1024)
        with pytest.raises(ValueError, match="[Ll]ength|[Ss]ize"):
            tokenize_cypher(oversized)

    def test_parser_accepts_within_limit(self) -> None:
        normal = "MATCH (n) WHERE n.tenant_id = $tenant_id RETURN n LIMIT 10"
        ast = CypherParser(normal).parse()
        assert ast.clauses


class TestCypherRecursionDepthLimit:
    def test_deeply_nested_call_subquery_rejected(self) -> None:
        query = "MATCH (n) "
        for _ in range(15):
            query += "CALL { "
        query += "RETURN 1"
        for _ in range(15):
            query += " }"
        with pytest.raises((ValueError, RecursionError)):
            CypherParser(query).parse()

    def test_moderate_nesting_accepted(self) -> None:
        query = "MATCH (n) CALL { CALL { RETURN 1 } } RETURN n"
        ast = CypherParser(query).parse()
        assert ast.clauses
