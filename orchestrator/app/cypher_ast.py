from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional

from orchestrator.app.cypher_tokenizer import (
    CypherToken,
    TokenType,
    tokenize_cypher,
)


@dataclass
class MatchClause:
    tokens: List[CypherToken] = field(default_factory=list)


@dataclass
class WhereClause:
    tokens: List[CypherToken] = field(default_factory=list)


@dataclass
class ReturnClause:
    tokens: List[CypherToken] = field(default_factory=list)


@dataclass
class WithClause:
    tokens: List[CypherToken] = field(default_factory=list)


@dataclass
class CallSubquery:
    tokens: List[CypherToken] = field(default_factory=list)
    body: List[Any] = field(default_factory=list)


@dataclass
class UnionQuery:
    tokens: List[CypherToken] = field(default_factory=list)
    branches: List[List[Any]] = field(default_factory=list)


@dataclass
class GenericClause:
    tokens: List[CypherToken] = field(default_factory=list)


@dataclass
class CypherAST:
    clauses: List[Any] = field(default_factory=list)
    raw_tokens: List[CypherToken] = field(default_factory=list)


_CLAUSE_KEYWORDS = frozenset({
    "MATCH", "WHERE", "RETURN", "WITH", "CALL", "UNION",
    "OPTIONAL", "UNWIND", "ORDER", "SKIP", "LIMIT",
})


class CypherParser:
    def __init__(self, cypher: str) -> None:
        self._tokens = tokenize_cypher(cypher)
        self._pos = 0

    def _current(self) -> Optional[CypherToken]:
        if self._pos < len(self._tokens):
            return self._tokens[self._pos]
        return None

    def _advance(self) -> CypherToken:
        token = self._tokens[self._pos]
        self._pos += 1
        return token

    def _peek_keyword(self) -> Optional[str]:
        for i in range(self._pos, len(self._tokens)):
            tok = self._tokens[i]
            if tok.token_type == TokenType.WHITESPACE:
                continue
            if tok.token_type == TokenType.KEYWORD:
                return tok.value.upper()
            return None
        return None

    def _skip_whitespace(self) -> List[CypherToken]:
        ws: List[CypherToken] = []
        while self._pos < len(self._tokens):
            if self._tokens[self._pos].token_type == TokenType.WHITESPACE:
                ws.append(self._advance())
            else:
                break
        return ws

    def parse(self) -> CypherAST:
        clauses = self._parse_clauses(top_level=True)
        return CypherAST(clauses=clauses, raw_tokens=self._tokens)

    def _dispatch_keyword(
        self, upper: str, clauses: List[Any],
    ) -> Any:
        dispatch = {
            "MATCH": self._parse_match,
            "OPTIONAL": self._parse_match,
            "WHERE": self._parse_where,
            "RETURN": self._parse_return,
            "WITH": self._parse_with,
            "CALL": self._parse_call,
        }
        parser = dispatch.get(upper)
        if parser is not None:
            return parser()
        if upper == "UNION":
            return self._parse_union(clauses)
        return self._parse_generic()

    def _parse_clauses(self, top_level: bool = False) -> List[Any]:
        clauses: List[Any] = []
        while self._pos < len(self._tokens):
            tok = self._current()
            if tok is None:
                break
            if tok.token_type == TokenType.WHITESPACE:
                self._advance()
                continue
            if (tok.token_type == TokenType.PUNCTUATION
                    and tok.value == "}" and not top_level):
                break
            if tok.token_type == TokenType.KEYWORD:
                clauses.append(
                    self._dispatch_keyword(tok.value.upper(), clauses)
                )
            else:
                clauses.append(self._parse_generic())
        return clauses

    def _collect_until_clause_keyword(self) -> List[CypherToken]:
        collected: List[CypherToken] = []
        while self._pos < len(self._tokens):
            tok = self._tokens[self._pos]
            if (tok.token_type == TokenType.KEYWORD
                    and tok.value.upper() in _CLAUSE_KEYWORDS
                    and tok.brace_depth == 0):
                break
            if (tok.token_type == TokenType.PUNCTUATION
                    and tok.value == "}"):
                break
            collected.append(self._advance())
        return collected

    def _parse_match(self) -> MatchClause:
        tokens = [self._advance()]
        tokens.extend(self._collect_until_clause_keyword())
        return MatchClause(tokens=tokens)

    def _parse_where(self) -> WhereClause:
        tokens = [self._advance()]
        tokens.extend(self._collect_until_clause_keyword())
        return WhereClause(tokens=tokens)

    def _parse_return(self) -> ReturnClause:
        tokens = [self._advance()]
        tokens.extend(self._collect_until_clause_keyword())
        return ReturnClause(tokens=tokens)

    def _parse_with(self) -> WithClause:
        tokens = [self._advance()]
        tokens.extend(self._collect_until_clause_keyword())
        return WithClause(tokens=tokens)

    def _parse_call(self) -> CallSubquery:
        call_token = self._advance()
        ws = self._skip_whitespace()
        tok = self._current()

        if tok and tok.token_type == TokenType.PUNCTUATION and tok.value == "{":
            self._advance()
            body = self._parse_clauses(top_level=False)
            if (self._pos < len(self._tokens)
                    and self._tokens[self._pos].value == "}"):
                self._advance()
            return CallSubquery(
                tokens=[call_token] + ws, body=body,
            )

        tokens = [call_token] + ws
        tokens.extend(self._collect_until_clause_keyword())
        return CallSubquery(tokens=tokens, body=[])

    def _parse_union(self, preceding: List[Any]) -> UnionQuery:
        union_token = self._advance()
        ws = self._skip_whitespace()
        all_token: List[CypherToken] = []
        if (self._current()
                and self._current().token_type == TokenType.KEYWORD
                and self._current().value.upper() == "ALL"):
            all_token = [self._advance()]
            ws.extend(self._skip_whitespace())

        rest = self._parse_clauses(top_level=True)
        return UnionQuery(
            tokens=[union_token] + all_token + ws,
            branches=[list(preceding), rest],
        )

    def _parse_generic(self) -> GenericClause:
        tokens = [self._advance()]
        tokens.extend(self._collect_until_clause_keyword())
        return GenericClause(tokens=tokens)


def _tokens_text(tokens: List[CypherToken]) -> str:
    return "".join(t.value for t in tokens)


def reconstruct_from_ast(ast: CypherAST) -> str:
    return _reconstruct_clauses(ast.clauses)


def _reconstruct_clauses(clauses: List[Any]) -> str:
    parts: List[str] = []
    for clause in clauses:
        if isinstance(clause, CallSubquery) and clause.body:
            parts.append(_tokens_text(clause.tokens))
            parts.append("{ ")
            parts.append(_reconstruct_clauses(clause.body))
            parts.append(" }")
        elif isinstance(clause, UnionQuery):
            parts.append(_tokens_text(clause.tokens))
            if len(clause.branches) > 1:
                parts.append(_reconstruct_clauses(clause.branches[1]))
        else:
            parts.append(_tokens_text(clause.tokens))
    return "".join(parts)


def inject_acl_all_scopes(cypher: str, acl_condition: str) -> str:
    ast = CypherParser(cypher).parse()
    _inject_into_clauses(ast.clauses, acl_condition)
    return reconstruct_from_ast(ast)


def _inject_into_clauses(clauses: List[Any], acl_condition: str) -> None:
    i = 0
    while i < len(clauses):
        clause = clauses[i]

        if isinstance(clause, CallSubquery) and clause.body:
            _inject_into_clauses(clause.body, acl_condition)

        if isinstance(clause, UnionQuery):
            for branch in clause.branches:
                _inject_into_clauses(branch, acl_condition)

        if isinstance(clause, MatchClause):
            next_idx = i + 1
            if (next_idx < len(clauses)
                    and isinstance(clauses[next_idx], WhereClause)):
                where = clauses[next_idx]
                where_text = _tokens_text(where.tokens)
                new_text = (
                    where_text.rstrip() + " AND " + acl_condition + " "
                )
                where.tokens = tokenize_cypher(new_text)
            else:
                where_tokens = tokenize_cypher(
                    " WHERE " + acl_condition + " "
                )
                clauses.insert(next_idx, WhereClause(tokens=where_tokens))
        i += 1
