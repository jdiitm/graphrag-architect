from __future__ import annotations

from typing import Any, Callable, List, Optional, Tuple

from orchestrator.app.cypher_ast import (
    CallSubquery,
    CypherParser,
    MatchClause,
    UnionQuery,
    WhereClause,
)


class UnfilteredCallMatchError(Exception):
    pass


class ASTVisitor:
    def __init__(self) -> None:
        self.on_match: Optional[Callable[[MatchClause, int], None]] = None
        self.on_call: Optional[Callable[[CallSubquery, int], None]] = None

    def walk(self, clauses: List[Any], depth: int = 0) -> None:
        for clause in clauses:
            if isinstance(clause, MatchClause):
                handler = self.on_match
                if handler is not None:
                    handler(clause, depth)
            if isinstance(clause, CallSubquery):
                call_handler = self.on_call
                if call_handler is not None:
                    call_handler(clause, depth)
                if clause.body:
                    self.walk(clause.body, depth + 1)
            if isinstance(clause, UnionQuery):
                for branch in clause.branches:
                    self.walk(branch, depth)


def _clauses_contain_field(
    clauses: List[Any], field_name: str,
) -> bool:
    for clause in clauses:
        if not isinstance(clause, WhereClause):
            continue
        text = "".join(t.value for t in clause.tokens)
        if field_name in text:
            return True
    return False


def _all_body_text_contains(
    clauses: List[Any], field_name: str,
) -> bool:
    for clause in clauses:
        tokens = getattr(clause, "tokens", [])
        text = "".join(t.value for t in tokens)
        if field_name in text:
            return True
    return False


def validate_call_subquery_acl(
    cypher: str,
    required_fields: Tuple[str, ...] = ("team_owner", "namespace_acl"),
) -> None:
    ast = CypherParser(cypher).parse()
    violations: List[str] = []

    def _check_call(call: CallSubquery, depth: int) -> None:
        if not call.body:
            return
        has_match = any(isinstance(c, MatchClause) for c in call.body)
        if not has_match:
            return
        for field_name in required_fields:
            if not _all_body_text_contains(call.body, field_name):
                violations.append(
                    f"CALL subquery at depth {depth} has MATCH without "
                    f"{field_name} filter"
                )
                return

    visitor = ASTVisitor()
    visitor.on_call = _check_call
    visitor.walk(ast.clauses)

    if violations:
        raise UnfilteredCallMatchError("; ".join(violations))


def count_acl_injection_depth(cypher: str) -> int:
    ast = CypherParser(cypher).parse()
    max_depth = 0

    def _track_call(call: CallSubquery, depth: int) -> None:
        nonlocal max_depth
        if call.body and any(isinstance(c, MatchClause) for c in call.body):
            max_depth = max(max_depth, depth + 1)

    visitor = ASTVisitor()
    visitor.on_call = _track_call
    visitor.walk(ast.clauses)
    return max_depth
