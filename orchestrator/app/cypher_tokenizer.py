from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Tuple


class TokenType(Enum):
    KEYWORD = "keyword"
    IDENTIFIER = "identifier"
    STRING_LITERAL = "string_literal"
    NUMBER = "number"
    PARAMETER = "parameter"
    PUNCTUATION = "punctuation"
    OPERATOR = "operator"
    WHITESPACE = "whitespace"
    COMMENT = "comment"


_CYPHER_KEYWORDS = frozenset({
    "MATCH", "OPTIONAL", "WHERE", "RETURN", "WITH", "UNWIND",
    "CALL", "YIELD", "UNION", "ALL", "CREATE", "MERGE", "DELETE",
    "DETACH", "SET", "REMOVE", "ORDER", "BY", "SKIP", "LIMIT",
    "ASC", "DESC", "AS", "AND", "OR", "NOT", "IN", "IS", "NULL",
    "TRUE", "FALSE", "CASE", "WHEN", "THEN", "ELSE", "END",
    "DISTINCT", "EXISTS", "CONTAINS", "STARTS", "ENDS", "LOAD",
    "CSV", "FOREACH", "ON", "DROP", "INDEX", "CONSTRAINT",
})


@dataclass(frozen=True)
class CypherToken:
    token_type: TokenType
    value: str
    position: int
    brace_depth: int


def _scan_line_comment(cypher: str, idx: int, depth: int) -> Tuple[CypherToken, int]:
    start = idx
    idx += 2
    while idx < len(cypher) and cypher[idx] != "\n":
        idx += 1
    return CypherToken(TokenType.COMMENT, cypher[start:idx], start, depth), idx


def _scan_block_comment(cypher: str, idx: int, depth: int) -> Tuple[CypherToken, int]:
    start = idx
    idx += 2
    while idx + 1 < len(cypher) and not (cypher[idx] == "*" and cypher[idx + 1] == "/"):
        idx += 1
    idx += 2
    return CypherToken(TokenType.COMMENT, cypher[start:idx], start, depth), idx


def _scan_string(cypher: str, idx: int, depth: int) -> Tuple[CypherToken, int]:
    start = idx
    quote = cypher[idx]
    idx += 1
    while idx < len(cypher):
        if cypher[idx] == "\\" and idx + 1 < len(cypher):
            idx += 2
            continue
        if cypher[idx] == quote:
            idx += 1
            break
        idx += 1
    return CypherToken(TokenType.STRING_LITERAL, cypher[start:idx], start, depth), idx


def _scan_backtick_id(cypher: str, idx: int, depth: int) -> Tuple[CypherToken, int]:
    start = idx
    idx += 1
    while idx < len(cypher) and cypher[idx] != "`":
        idx += 1
    if idx < len(cypher):
        idx += 1
    return CypherToken(TokenType.IDENTIFIER, cypher[start:idx], start, depth), idx


def _scan_parameter(cypher: str, idx: int, depth: int) -> Tuple[CypherToken, int]:
    start = idx
    idx += 1
    while idx < len(cypher) and (cypher[idx].isalnum() or cypher[idx] == "_"):
        idx += 1
    return CypherToken(TokenType.PARAMETER, cypher[start:idx], start, depth), idx


def _scan_word(
    cypher: str, idx: int, depth: int, prior_tokens: List[CypherToken],
) -> Tuple[CypherToken, int]:
    start = idx
    while idx < len(cypher) and (cypher[idx].isalnum() or cypher[idx] == "_"):
        idx += 1
    word = cypher[start:idx]
    is_property = _preceded_by_dot(prior_tokens)
    if not is_property and word.upper() in _CYPHER_KEYWORDS:
        return CypherToken(TokenType.KEYWORD, word, start, depth), idx
    return CypherToken(TokenType.IDENTIFIER, word, start, depth), idx


def _preceded_by_dot(tokens: List[CypherToken]) -> bool:
    for prev in reversed(tokens):
        if prev.token_type == TokenType.WHITESPACE:
            continue
        return prev.token_type == TokenType.PUNCTUATION and prev.value == "."
    return False


class _Lexer:
    def __init__(self, cypher: str) -> None:
        self.cypher = cypher
        self.length = len(cypher)
        self.idx = 0
        self.brace_depth = 0
        self.tokens: List[CypherToken] = []

    def _char(self) -> str:
        return self.cypher[self.idx]

    def _peek(self) -> str:
        return self.cypher[self.idx + 1] if self.idx + 1 < self.length else ""

    def _emit(self, token: CypherToken) -> None:
        self.tokens.append(token)

    def _try_comment(self) -> bool:
        if self._char() != "/" or self.idx + 1 >= self.length:
            return False
        nxt = self._peek()
        if nxt == "/":
            token, self.idx = _scan_line_comment(self.cypher, self.idx, self.brace_depth)
            self._emit(token)
            return True
        if nxt == "*":
            token, self.idx = _scan_block_comment(self.cypher, self.idx, self.brace_depth)
            self._emit(token)
            return True
        return False

    def _try_string_or_backtick(self) -> bool:
        char = self._char()
        if char in ("'", '"'):
            token, self.idx = _scan_string(self.cypher, self.idx, self.brace_depth)
            self._emit(token)
            return True
        if char == "`":
            token, self.idx = _scan_backtick_id(self.cypher, self.idx, self.brace_depth)
            self._emit(token)
            return True
        return False

    def _try_parameter(self) -> bool:
        if self._char() != "$" or self.idx + 1 >= self.length:
            return False
        nxt = self._peek()
        if nxt.isalpha() or nxt == "_":
            token, self.idx = _scan_parameter(self.cypher, self.idx, self.brace_depth)
            self._emit(token)
            return True
        return False

    def _try_brace(self) -> bool:
        char = self._char()
        if char == "{":
            self.brace_depth += 1
            self._emit(CypherToken(TokenType.PUNCTUATION, char, self.idx, self.brace_depth))
            self.idx += 1
            return True
        if char == "}":
            self._emit(CypherToken(TokenType.PUNCTUATION, char, self.idx, self.brace_depth))
            self.brace_depth = max(0, self.brace_depth - 1)
            self.idx += 1
            return True
        return False

    def _try_simple(self) -> bool:
        char = self._char()
        if char in " \t\n\r":
            start = self.idx
            while self.idx < self.length and self.cypher[self.idx] in " \t\n\r":
                self.idx += 1
            ws_val = self.cypher[start:self.idx]
            self._emit(CypherToken(TokenType.WHITESPACE, ws_val, start, self.brace_depth))
            return True
        if char in "()[],:;":
            self._emit(CypherToken(TokenType.PUNCTUATION, char, self.idx, self.brace_depth))
            self.idx += 1
            return True
        if char.isdigit() or (char == "." and self._peek().isdigit()):
            start = self.idx
            while self.idx < self.length and self.cypher[self.idx] in "0123456789.":
                self.idx += 1
            num_val = self.cypher[start:self.idx]
            self._emit(CypherToken(TokenType.NUMBER, num_val, start, self.brace_depth))
            return True
        return False

    def _try_word_or_operator(self) -> None:
        char = self._char()
        if char.isalpha() or char == "_":
            token, self.idx = _scan_word(self.cypher, self.idx, self.brace_depth, self.tokens)
            self._emit(token)
        elif char in "=<>!+-*/%^":
            start = self.idx
            self.idx += 1
            if self.idx < self.length and self.cypher[self.idx] in "=<>":
                self.idx += 1
            op_val = self.cypher[start:self.idx]
            self._emit(CypherToken(TokenType.OPERATOR, op_val, start, self.brace_depth))
        elif char == ".":
            self._emit(CypherToken(TokenType.PUNCTUATION, char, self.idx, self.brace_depth))
            self.idx += 1
        else:
            self._emit(CypherToken(TokenType.IDENTIFIER, char, self.idx, self.brace_depth))
            self.idx += 1

    def run(self) -> List[CypherToken]:
        while self.idx < self.length:
            if self._try_comment():
                continue
            if self._try_string_or_backtick():
                continue
            if self._try_parameter():
                continue
            if self._try_brace():
                continue
            if self._try_simple():
                continue
            self._try_word_or_operator()
        return self.tokens


def tokenize_cypher(cypher: str) -> List[CypherToken]:
    return _Lexer(cypher).run()


def find_toplevel_keyword_positions(
    tokens: List[CypherToken],
) -> dict[str, List[int]]:
    positions: dict[str, List[int]] = {}
    for i, token in enumerate(tokens):
        if token.token_type == TokenType.KEYWORD and token.brace_depth == 0:
            key = token.value.upper()
            positions.setdefault(key, []).append(i)
    return positions


def find_union_boundaries(
    tokens: List[CypherToken],
) -> List[int]:
    return [
        i for i, t in enumerate(tokens)
        if t.token_type == TokenType.KEYWORD
        and t.value.upper() == "UNION"
        and t.brace_depth == 0
    ]


def reconstruct_cypher(tokens: List[CypherToken]) -> str:
    return "".join(t.value for t in tokens)
