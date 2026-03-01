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
class UnwindClause:
    tokens: List[CypherToken] = field(default_factory=list)
    expression_text: Optional[str] = None


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
            "UNWIND": self._parse_unwind,
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

    def _collect_until_clause_keyword(
        self, scope_depth: int = 0,
    ) -> List[CypherToken]:
        collected: List[CypherToken] = []
        while self._pos < len(self._tokens):
            tok = self._tokens[self._pos]
            if (tok.token_type == TokenType.KEYWORD
                    and tok.value.upper() in _CLAUSE_KEYWORDS
                    and tok.brace_depth == scope_depth):
                break
            if (tok.token_type == TokenType.PUNCTUATION
                    and tok.value == "}"
                    and tok.brace_depth <= scope_depth):
                break
            collected.append(self._advance())
        return collected

    def _parse_match(self) -> MatchClause:
        kw_tok = self._advance()
        tokens = [kw_tok]
        tokens.extend(self._collect_until_clause_keyword(kw_tok.brace_depth))
        return MatchClause(tokens=tokens)

    def _parse_where(self) -> WhereClause:
        kw_tok = self._advance()
        tokens = [kw_tok]
        tokens.extend(self._collect_until_clause_keyword(kw_tok.brace_depth))
        return WhereClause(tokens=tokens)

    def _parse_return(self) -> ReturnClause:
        kw_tok = self._advance()
        tokens = [kw_tok]
        tokens.extend(self._collect_until_clause_keyword(kw_tok.brace_depth))
        return ReturnClause(tokens=tokens)

    def _parse_with(self) -> WithClause:
        kw_tok = self._advance()
        tokens = [kw_tok]
        tokens.extend(self._collect_until_clause_keyword(kw_tok.brace_depth))
        return WithClause(tokens=tokens)

    def _parse_unwind(self) -> UnwindClause:
        kw_tok = self._advance()
        tokens = [kw_tok]
        body_tokens = self._collect_until_clause_keyword(kw_tok.brace_depth)
        tokens.extend(body_tokens)
        expr_parts = []
        for t in body_tokens:
            if (t.token_type == TokenType.KEYWORD
                    and t.value.upper() == "AS"):
                break
            expr_parts.append(t.value)
        expression_text = "".join(expr_parts).strip() or None
        return UnwindClause(tokens=tokens, expression_text=expression_text)

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
        kw_tok = self._advance()
        tokens = [kw_tok]
        tokens.extend(self._collect_until_clause_keyword(kw_tok.brace_depth))
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
    injected = False
    i = 0
    while i < len(clauses):
        clause = clauses[i]

        if isinstance(clause, CallSubquery) and clause.body:
            _inject_into_clauses(clause.body, acl_condition)

        if isinstance(clause, UnionQuery):
            for branch in clause.branches:
                _inject_into_clauses(branch, acl_condition)

        if isinstance(clause, MatchClause):
            injected = True
            next_idx = i + 1
            if (next_idx < len(clauses)
                    and isinstance(clauses[next_idx], WhereClause)):
                where = clauses[next_idx]
                where_text = _tokens_text(where.tokens)
                body = _strip_where_keyword(where_text)
                new_text = (
                    " WHERE " + acl_condition
                    + " AND (" + body.strip() + ") "
                )
                where.tokens = tokenize_cypher(new_text)
            else:
                where_tokens = tokenize_cypher(
                    " WHERE " + acl_condition + " "
                )
                clauses.insert(next_idx, WhereClause(tokens=where_tokens))
        i += 1

    if not injected:
        has_procedure_call = any(
            isinstance(c, CallSubquery) and not c.body
            for c in clauses
        )
        if has_procedure_call:
            _inject_before_return(clauses, acl_condition)


def _inject_before_return(
    clauses: List[Any], acl_condition: str,
) -> None:
    for i, clause in enumerate(clauses):
        if isinstance(clause, ReturnClause):
            where_tokens = tokenize_cypher(
                " WHERE " + acl_condition + " "
            )
            clauses.insert(i, WhereClause(tokens=where_tokens))
            return


def validate_acl_coverage(cypher: str, acl_marker: str) -> bool:
    ast = CypherParser(cypher).parse()
    return _all_matches_have_acl(ast.clauses, acl_marker)


def _all_matches_have_acl(clauses: List[Any], marker: str) -> bool:
    i = 0
    while i < len(clauses):
        clause = clauses[i]
        if isinstance(clause, CallSubquery) and clause.body:
            if not _all_matches_have_acl(clause.body, marker):
                return False
        if isinstance(clause, UnionQuery):
            for branch in clause.branches:
                if not _all_matches_have_acl(branch, marker):
                    return False
        if isinstance(clause, MatchClause):
            match_text = _tokens_text(clause.tokens)
            if marker in match_text:
                i += 1
                continue
            next_idx = i + 1
            if next_idx < len(clauses) and isinstance(clauses[next_idx], WhereClause):
                where_text = _tokens_text(clauses[next_idx].tokens)
                if marker not in where_text:
                    return False
            else:
                return False
        i += 1
    return True


def _strip_where_keyword(text: str) -> str:
    stripped = text.lstrip()
    if stripped.upper().startswith("WHERE"):
        return stripped[5:]
    return text


def _is_limit_clause(clause: Any) -> bool:
    if not hasattr(clause, "tokens") or not clause.tokens:
        return False
    for t in clause.tokens:
        if t.token_type == TokenType.WHITESPACE:
            continue
        return (
            t.token_type == TokenType.KEYWORD
            and t.value.upper() == "LIMIT"
        )
    return False


def _has_amplification_in_clauses(clauses: List[Any]) -> bool:
    seen_with = False
    seen_limit_after_with = False

    for clause in clauses:
        if isinstance(clause, WithClause):
            seen_with = True
            continue

        if seen_with and _is_limit_clause(clause):
            seen_limit_after_with = True
            continue

        if isinstance(clause, CallSubquery) and clause.body:
            if seen_limit_after_with and _has_unwind_in_clauses(clause.body):
                return True
            if _has_amplification_in_clauses(clause.body):
                return True

        if isinstance(clause, UnwindClause) and seen_limit_after_with:
            return True

        if isinstance(clause, GenericClause):
            has_unwind = any(
                t.token_type == TokenType.KEYWORD
                and t.value.upper() == "UNWIND"
                for t in clause.tokens
            )
            if has_unwind and seen_limit_after_with:
                return True

    return False


def _has_unwind_in_clauses(clauses: List[Any]) -> bool:
    for clause in clauses:
        if isinstance(clause, UnwindClause):
            return True
        if isinstance(clause, GenericClause):
            if any(
                t.token_type == TokenType.KEYWORD
                and t.value.upper() == "UNWIND"
                for t in clause.tokens
            ):
                return True
        if isinstance(clause, CallSubquery) and clause.body:
            if _has_unwind_in_clauses(clause.body):
                return True
    return False


def validate_query_structure(cypher: str) -> bool:
    ast = CypherParser(cypher).parse()
    return not _has_amplification_in_clauses(ast.clauses)


def _extract_limit_value(clause: Any) -> Optional[int]:
    if not _is_limit_clause(clause):
        return None
    for t in clause.tokens:
        if t.token_type == TokenType.NUMBER:
            try:
                return int(t.value)
            except ValueError:
                return None
    return None


def _set_limit_value(clause: Any, value: int) -> None:
    for i, t in enumerate(clause.tokens):
        if t.token_type == TokenType.NUMBER:
            clause.tokens[i] = CypherToken(
                token_type=TokenType.NUMBER,
                value=str(value),
                position=t.position,
                brace_depth=t.brace_depth,
            )
            return


def _cap_limits_in_clauses(clauses: List[Any], max_results: int) -> bool:
    found_limit = False
    for clause in clauses:
        if isinstance(clause, CallSubquery) and clause.body:
            _cap_limits_in_clauses(clause.body, max_results)

        if isinstance(clause, UnionQuery):
            for branch in clause.branches:
                _cap_limits_in_clauses(branch, max_results)

        current_value = _extract_limit_value(clause)
        if current_value is not None:
            found_limit = True
            if current_value > max_results:
                _set_limit_value(clause, max_results)

    return found_limit


def inject_limit_ast(cypher: str, max_results: int) -> str:
    ast = CypherParser(cypher).parse()
    found = _cap_limits_in_clauses(ast.clauses, max_results)
    if not found:
        limit_tokens = tokenize_cypher(f" LIMIT {max_results}")
        ast.clauses.append(GenericClause(tokens=limit_tokens))
    return reconstruct_from_ast(ast)
