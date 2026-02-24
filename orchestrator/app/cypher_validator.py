from typing import FrozenSet

from orchestrator.app.cypher_tokenizer import (
    TokenType,
    tokenize_cypher,
)

_WRITE_KEYWORDS: FrozenSet[str] = frozenset({
    "MERGE", "CREATE", "DELETE", "SET", "REMOVE", "DROP",
})

_DETACH_KEYWORD = "DETACH"

ALLOWED_PROCEDURES: FrozenSet[str] = frozenset({
    "db.index.fulltext.queryNodes",
    "db.index.fulltext.queryRelationships",
    "db.labels",
    "db.relationshipTypes",
    "db.propertyKeys",
    "db.schema.visualization",
    "db.schema.nodeTypeProperties",
    "db.schema.relTypeProperties",
    "dbms.components",
    "dbms.queryJmx",
})


class CypherValidationError(ValueError):
    pass


def _next_non_whitespace(tokens: list, start: int) -> tuple:
    for ahead in tokens[start:]:
        if ahead.token_type != TokenType.WHITESPACE:
            return ahead.token_type, ahead.value.upper()
    return None, ""


def _check_detach_delete(tokens: list, idx: int, raw: str) -> None:
    tok_type, tok_val = _next_non_whitespace(tokens, idx + 1)
    if tok_type == TokenType.KEYWORD and tok_val == "DELETE":
        raise CypherValidationError(
            f"Cypher contains write operation: {raw[:80]}"
        )


def _check_load_csv(tokens: list, idx: int, raw: str) -> None:
    tok_type, tok_val = _next_non_whitespace(tokens, idx + 1)
    if tok_type == TokenType.KEYWORD and tok_val == "CSV":
        raise CypherValidationError(
            f"Cypher contains LOAD CSV: {raw[:80]}"
        )


def validate_cypher_readonly(cypher: str) -> str:
    stripped = cypher.strip()
    tokens = tokenize_cypher(stripped)

    for i, token in enumerate(tokens):
        if token.token_type != TokenType.KEYWORD or token.brace_depth != 0:
            continue

        upper = token.value.upper()

        if upper in _WRITE_KEYWORDS:
            raise CypherValidationError(
                f"Cypher contains write operation: {stripped[:80]}"
            )
        if upper == _DETACH_KEYWORD:
            _check_detach_delete(tokens, i, stripped)
        elif upper == "LOAD":
            _check_load_csv(tokens, i, stripped)
        elif upper == "CALL":
            _validate_call_token(tokens, i, stripped)

    _check_multi_statement(tokens, stripped)

    return stripped


def _validate_call_token(tokens: list, call_idx: int, raw: str) -> None:
    j = call_idx + 1
    while j < len(tokens) and tokens[j].token_type == TokenType.WHITESPACE:
        j += 1

    if j >= len(tokens):
        return

    if tokens[j].token_type == TokenType.PUNCTUATION and tokens[j].value == "{":
        raise CypherValidationError(
            f"Cypher contains CALL subquery: {raw[:80]}"
        )

    if tokens[j].token_type == TokenType.IDENTIFIER:
        procedure_parts = [tokens[j].value]
        k = j + 1
        while k + 1 < len(tokens):
            if (tokens[k].token_type == TokenType.PUNCTUATION
                    and tokens[k].value == "."
                    and tokens[k + 1].token_type == TokenType.IDENTIFIER):
                procedure_parts.append(tokens[k + 1].value)
                k += 2
            else:
                break
        procedure_name = ".".join(procedure_parts)
        if procedure_name not in ALLOWED_PROCEDURES:
            raise CypherValidationError(
                f"Cypher calls disallowed procedure '{procedure_name}': "
                f"{raw[:80]}"
            )


def _check_multi_statement(tokens: list, raw: str) -> None:
    for i, token in enumerate(tokens):
        if token.token_type != TokenType.PUNCTUATION or token.value != ";":
            continue
        for ahead in tokens[i + 1:]:
            if ahead.token_type == TokenType.WHITESPACE:
                continue
            raise CypherValidationError(
                f"Cypher contains multiple statements: {raw[:80]}"
            )
