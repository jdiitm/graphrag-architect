import pytest

from orchestrator.app.cypher_validator import (
    CypherValidationError,
    validate_cypher_readonly,
)


class TestRejectsWriteKeywords:
    def test_rejects_merge(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly("MERGE (n:Service {id: '1'}) RETURN n")

    def test_rejects_create(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly("CREATE (n:Service {id: '1'})")

    def test_rejects_delete(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly("MATCH (n) DELETE n")

    def test_rejects_detach_delete(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly("MATCH (n) DETACH DELETE n")

    def test_rejects_set(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly("MATCH (n:Service) SET n.name = 'x' RETURN n")

    def test_rejects_remove(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly("MATCH (n) REMOVE n.prop RETURN n")

    def test_rejects_drop(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly("DROP INDEX my_index")

    def test_rejects_call_subquery(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly(
                "CALL { CREATE (n:Service {id: '1'}) } RETURN n"
            )

    def test_rejects_case_insensitive(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly("match (n) detach delete n")


class TestAllowsReadKeywords:
    def test_allows_match_return(self):
        result = validate_cypher_readonly("MATCH (n:Service) RETURN n")
        assert result == "MATCH (n:Service) RETURN n"

    def test_allows_where(self):
        result = validate_cypher_readonly(
            "MATCH (n:Service) WHERE n.name = 'auth' RETURN n"
        )
        assert "WHERE" in result

    def test_allows_optional_match(self):
        result = validate_cypher_readonly(
            "OPTIONAL MATCH (n)-[r]->(m) RETURN n, r, m"
        )
        assert "OPTIONAL MATCH" in result

    def test_allows_order_by_limit(self):
        result = validate_cypher_readonly(
            "MATCH (n) RETURN n ORDER BY n.name LIMIT 10"
        )
        assert result.endswith("LIMIT 10")

    def test_allows_with_clause(self):
        result = validate_cypher_readonly(
            "MATCH (n) WITH n.name AS name RETURN name"
        )
        assert "WITH" in result

    def test_allows_union(self):
        result = validate_cypher_readonly(
            "MATCH (n:Service) RETURN n.name UNION MATCH (m:Database) RETURN m.type"
        )
        assert "UNION" in result

    def test_allows_fulltext_call(self):
        result = validate_cypher_readonly(
            "CALL db.index.fulltext.queryNodes('idx', 'auth') "
            "YIELD node, score RETURN node"
        )
        assert "CALL db.index.fulltext" in result

    def test_allows_count_aggregation(self):
        result = validate_cypher_readonly(
            "MATCH (n:Service)-[:CALLS]->(m) RETURN n.name, count(m)"
        )
        assert "count" in result

    def test_returns_stripped_input(self):
        result = validate_cypher_readonly("  MATCH (n) RETURN n  ")
        assert result == "MATCH (n) RETURN n"
