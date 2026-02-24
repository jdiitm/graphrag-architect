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


class TestRejectsApocProcedureCalls:
    def test_rejects_apoc_load_csv(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly(
                "CALL apoc.load.csv('file:///proc/self/environ') "
                "YIELD lineNo, list RETURN list"
            )

    def test_rejects_apoc_import_csv(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly(
                "CALL apoc.import.csv([{fileName: 'f.csv'}], [], {})"
            )

    def test_rejects_apoc_load_json(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly(
                "CALL apoc.load.json('file:///etc/passwd') YIELD value RETURN value"
            )

    def test_rejects_apoc_text_function(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly(
                "CALL apoc.text.join(['a','b'], ',') YIELD value RETURN value"
            )

    def test_rejects_generic_call_procedure(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly(
                "CALL some.unknown.procedure() YIELD x RETURN x"
            )

    def test_rejects_call_procedure_case_insensitive(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly(
                "call APOC.LOAD.CSV('file:///tmp/secret') YIELD lineNo RETURN lineNo"
            )


class TestRejectsLoadCsv:
    def test_rejects_load_csv(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly(
                "LOAD CSV FROM 'file:///etc/passwd' AS row RETURN row"
            )

    def test_rejects_load_csv_with_headers(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly(
                "LOAD CSV WITH HEADERS FROM 'file:///data.csv' AS row RETURN row"
            )

    def test_rejects_load_csv_case_insensitive(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly(
                "load csv from 'file:///etc/shadow' as row return row"
            )


class TestAllowlistProcedures:
    def test_allows_db_index_fulltext_query_nodes(self):
        result = validate_cypher_readonly(
            "CALL db.index.fulltext.queryNodes('idx', 'auth') "
            "YIELD node, score RETURN node"
        )
        assert "CALL db.index.fulltext.queryNodes" in result

    def test_allows_db_index_fulltext_query_relationships(self):
        result = validate_cypher_readonly(
            "CALL db.index.fulltext.queryRelationships('idx', 'calls') "
            "YIELD relationship, score RETURN relationship"
        )
        assert "CALL db.index.fulltext.queryRelationships" in result

    def test_allows_db_labels(self):
        result = validate_cypher_readonly(
            "CALL db.labels() YIELD label RETURN label"
        )
        assert "CALL db.labels" in result

    def test_allows_db_relationship_types(self):
        result = validate_cypher_readonly(
            "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
        )
        assert "CALL db.relationshipTypes" in result

    def test_allows_db_property_keys(self):
        result = validate_cypher_readonly(
            "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey"
        )
        assert "CALL db.propertyKeys" in result

    def test_allows_db_schema_visualization(self):
        result = validate_cypher_readonly(
            "CALL db.schema.visualization() YIELD nodes, relationships RETURN nodes"
        )
        assert "CALL db.schema.visualization" in result

    def test_allows_dbms_components(self):
        result = validate_cypher_readonly(
            "CALL dbms.components() YIELD name, versions RETURN name"
        )
        assert "CALL dbms.components" in result


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


class TestCartesianProductDetection:
    def test_rejects_comma_separated_match_patterns(self):
        with pytest.raises(CypherValidationError, match="[Cc]artesian"):
            validate_cypher_readonly("MATCH (a), (b) RETURN a, b")

    def test_rejects_multi_node_comma_pattern(self):
        with pytest.raises(CypherValidationError, match="[Cc]artesian"):
            validate_cypher_readonly("MATCH (n:Service), (m:Database) RETURN n, m")

    def test_allows_connected_match_pattern(self):
        result = validate_cypher_readonly("MATCH (a)-[r]-(b) RETURN a, b")
        assert "MATCH" in result

    def test_allows_single_node_match(self):
        result = validate_cypher_readonly("MATCH (n:Service) RETURN n")
        assert "MATCH" in result

    def test_allows_multiple_match_clauses_with_where(self):
        result = validate_cypher_readonly(
            "MATCH (a:Service) MATCH (b:Database) WHERE b.name = a.db RETURN a, b"
        )
        assert "MATCH" in result

    def test_rejects_three_disconnected_nodes(self):
        with pytest.raises(CypherValidationError, match="[Cc]artesian"):
            validate_cypher_readonly("MATCH (a), (b), (c) RETURN a, b, c")

    def test_allows_path_with_multiple_relationships(self):
        result = validate_cypher_readonly(
            "MATCH (a)-[:CALLS]->(b)-[:PRODUCES]->(c) RETURN a, c"
        )
        assert "MATCH" in result


class TestStringLiteralsNotFalsePositive:
    def test_allows_delete_keyword_inside_string_literal(self):
        result = validate_cypher_readonly(
            "MATCH (n) WHERE n.desc = 'DELETE WHERE RETURN' RETURN n"
        )
        assert "'DELETE WHERE RETURN'" in result

    def test_allows_create_keyword_inside_double_quoted_string(self):
        result = validate_cypher_readonly(
            'MATCH (n) WHERE n.desc = "CREATE TABLE users" RETURN n'
        )
        assert '"CREATE TABLE users"' in result

    def test_allows_set_keyword_inside_string_literal(self):
        result = validate_cypher_readonly(
            "MATCH (n) WHERE n.name = 'SET_HANDLER' RETURN n"
        )
        assert "'SET_HANDLER'" in result

    def test_allows_merge_keyword_inside_string_literal(self):
        result = validate_cypher_readonly(
            "MATCH (n) WHERE n.operation = 'MERGE sort' RETURN n"
        )
        assert "'MERGE sort'" in result

    def test_allows_case_expression_with_set_in_branch(self):
        result = validate_cypher_readonly(
            "MATCH (n:Service) "
            "RETURN CASE WHEN n.language = 'Go' THEN 'backend' ELSE 'other' END"
        )
        assert "CASE WHEN" in result

    def test_rejects_real_create_not_fooled_by_string(self):
        with pytest.raises(CypherValidationError):
            validate_cypher_readonly(
                "CREATE (n:Evil {name: 'injected'}) RETURN n"
            )

    def test_allows_drop_keyword_inside_string_property(self):
        result = validate_cypher_readonly(
            "MATCH (n) WHERE n.action = 'DROP TABLE' RETURN n"
        )
        assert "'DROP TABLE'" in result
