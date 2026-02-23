import pytest

from orchestrator.app.cypher_tokenizer import (
    CypherToken,
    TokenType,
    tokenize_cypher,
)


class TestTokenizerBasics:
    def test_tokenizes_simple_match_return(self):
        tokens = tokenize_cypher("MATCH (n) RETURN n")
        keywords = [t for t in tokens if t.token_type == TokenType.KEYWORD]
        keyword_values = [k.value.upper() for k in keywords]
        assert "MATCH" in keyword_values
        assert "RETURN" in keyword_values

    def test_tracks_brace_depth(self):
        tokens = tokenize_cypher("CALL { MATCH (n) WHERE n.x = 1 RETURN n }")
        inner_where = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD
            and t.value.upper() == "WHERE"
        ]
        assert len(inner_where) == 1
        assert inner_where[0].brace_depth == 1

    def test_string_literals_not_tokenized_as_keywords(self):
        tokens = tokenize_cypher("MATCH (n) WHERE n.name = 'WHERE' RETURN n")
        where_keywords = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD and t.value.upper() == "WHERE"
        ]
        assert len(where_keywords) == 1

    def test_double_quoted_strings_preserved(self):
        tokens = tokenize_cypher('MATCH (n) WHERE n.name = "RETURN" RETURN n')
        return_keywords = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD and t.value.upper() == "RETURN"
        ]
        assert len(return_keywords) == 1

    def test_single_line_comments_skipped(self):
        cypher = "MATCH (n) // WHERE fake\nWHERE n.x = 1 RETURN n"
        tokens = tokenize_cypher(cypher)
        where_keywords = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD and t.value.upper() == "WHERE"
        ]
        assert len(where_keywords) == 1

    def test_block_comments_skipped(self):
        cypher = "MATCH (n) /* WHERE fake RETURN fake */ WHERE n.x = 1 RETURN n"
        tokens = tokenize_cypher(cypher)
        where_keywords = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD and t.value.upper() == "WHERE"
        ]
        assert len(where_keywords) == 1

    def test_nested_braces_tracked_correctly(self):
        cypher = "CALL { CALL { MATCH (n) RETURN n } RETURN n }"
        tokens = tokenize_cypher(cypher)
        returns = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD and t.value.upper() == "RETURN"
        ]
        assert returns[0].brace_depth == 2
        assert returns[1].brace_depth == 1

    def test_escaped_quotes_inside_strings(self):
        cypher = r"MATCH (n) WHERE n.name = 'it\'s WHERE' RETURN n"
        tokens = tokenize_cypher(cypher)
        where_keywords = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD and t.value.upper() == "WHERE"
        ]
        assert len(where_keywords) == 1

    def test_parameters_recognized(self):
        tokens = tokenize_cypher("MATCH (n) WHERE n.name = $name RETURN n")
        params = [t for t in tokens if t.token_type == TokenType.PARAMETER]
        assert len(params) == 1
        assert params[0].value == "$name"


class TestTokenizerDefeatVectors:
    def test_union_creates_separate_clause_scopes(self):
        cypher = (
            "MATCH (n:Service) RETURN n "
            "UNION "
            "MATCH (m:Service) RETURN m"
        )
        tokens = tokenize_cypher(cypher)
        unions = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD and t.value.upper() == "UNION"
        ]
        assert len(unions) == 1
        returns = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD and t.value.upper() == "RETURN"
        ]
        assert len(returns) == 2

    def test_with_clause_recognized(self):
        cypher = "MATCH (n) WITH n WHERE n.x = 1 RETURN n"
        tokens = tokenize_cypher(cypher)
        withs = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD and t.value.upper() == "WITH"
        ]
        assert len(withs) == 1

    def test_property_key_named_where_not_keyword(self):
        cypher = "MATCH (n) WHERE n.where = 'yes' RETURN n"
        tokens = tokenize_cypher(cypher)
        where_keywords = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD and t.value.upper() == "WHERE"
        ]
        assert len(where_keywords) == 1

    def test_backtick_quoted_identifiers(self):
        cypher = "MATCH (n) WHERE n.`WHERE` = 1 RETURN n"
        tokens = tokenize_cypher(cypher)
        where_keywords = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD and t.value.upper() == "WHERE"
        ]
        assert len(where_keywords) == 1
