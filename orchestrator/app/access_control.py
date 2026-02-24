from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import jwt

from orchestrator.app.call_isolation import (
    UnfilteredCallMatchError,
    validate_call_subquery_acl,
)
from orchestrator.app.cypher_tokenizer import (
    CypherToken,
    TokenType,
    find_union_boundaries,
    reconstruct_cypher,
    tokenize_cypher,
)

_acl_logger = logging.getLogger(__name__)

DEFAULT_TOKEN_TTL_SECONDS = 3600
JWT_ALGORITHM = "HS256"


class InvalidTokenError(Exception):
    pass


class AuthConfigurationError(Exception):
    pass


def sign_token(
    claims: Dict[str, Any],
    secret: str,
    ttl_seconds: int = DEFAULT_TOKEN_TTL_SECONDS,
) -> str:
    now = int(time.time())
    payload = {
        **claims,
        "iat": now,
        "exp": now + ttl_seconds,
    }
    return jwt.encode(payload, secret, algorithm=JWT_ALGORITHM)


def _verify_token(token: str, secret: str) -> Dict[str, Any]:
    try:
        return jwt.decode(token, secret, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError as exc:
        raise InvalidTokenError("token expired") from exc
    except jwt.InvalidTokenError as exc:
        raise InvalidTokenError(f"token invalid: {exc}") from exc


@dataclass(frozen=True)
class SecurityPrincipal:
    team: str
    namespace: str
    role: str

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    @classmethod
    def from_header(
        cls,
        header: Optional[str],
        token_secret: str = "",
        require_verification: bool = True,
    ) -> SecurityPrincipal:
        if not header or not header.strip():
            return cls(team="*", namespace="*", role="anonymous")

        token = header.removeprefix("Bearer ").strip()

        if token_secret:
            claims = _verify_token(token, token_secret)
        elif require_verification:
            raise InvalidTokenError(
                "token provided but no secret configured for verification"
            )
        else:
            return cls(team="*", namespace="*", role="anonymous")

        return cls(
            team=claims.get("team", "*"),
            namespace=claims.get("namespace", "*"),
            role=claims.get("role", "viewer"),
        )


def _find_toplevel_keyword_indices(
    tokens: List[CypherToken],
) -> Tuple[int, int]:
    first_where = -1
    first_return = -1
    for i, token in enumerate(tokens):
        if token.token_type != TokenType.KEYWORD or token.brace_depth != 0:
            continue
        upper = token.value.upper()
        if upper == "WHERE" and first_where < 0:
            first_where = i
        if upper == "RETURN" and first_return < 0:
            first_return = i
    return first_where, first_return


def _inject_acl_into_clause(
    tokens: List[CypherToken],
    node_clause: str,
) -> List[CypherToken]:
    where_idx, return_idx = _find_toplevel_keyword_indices(tokens)

    depth = 0
    acl_tokens = [
        CypherToken(TokenType.WHITESPACE, " ", 0, depth),
        CypherToken(TokenType.IDENTIFIER, node_clause, 0, depth),
    ]

    if where_idx >= 0:
        after_where_idx = where_idx + 1
        while (after_where_idx < len(tokens)
               and tokens[after_where_idx].token_type == TokenType.WHITESPACE):
            after_where_idx += 1

        if return_idx > where_idx:
            existing_start = after_where_idx
            existing_end = return_idx
        else:
            existing_start = after_where_idx
            existing_end = len(tokens)

        existing_tokens = tokens[existing_start:existing_end]
        while existing_tokens and existing_tokens[-1].token_type == TokenType.WHITESPACE:
            existing_tokens = existing_tokens[:-1]

        result = list(tokens[:where_idx + 1])
        result.extend(acl_tokens)
        result.append(CypherToken(TokenType.WHITESPACE, " ", 0, depth))
        result.append(CypherToken(TokenType.KEYWORD, "AND", 0, depth))
        result.append(CypherToken(TokenType.WHITESPACE, " ", 0, depth))
        result.append(CypherToken(TokenType.PUNCTUATION, "(", 0, depth))
        result.extend(existing_tokens)
        result.append(CypherToken(TokenType.PUNCTUATION, ")", 0, depth))
        result.append(CypherToken(TokenType.WHITESPACE, " ", 0, depth))
        if return_idx > where_idx:
            result.extend(tokens[return_idx:])
        return result

    if return_idx >= 0:
        result = list(tokens[:return_idx])
        result.append(CypherToken(TokenType.KEYWORD, "WHERE", 0, depth))
        result.extend(acl_tokens)
        result.append(CypherToken(TokenType.WHITESPACE, " ", 0, depth))
        result.extend(tokens[return_idx:])
        return result

    result = list(tokens)
    result.append(CypherToken(TokenType.WHITESPACE, " ", 0, depth))
    result.append(CypherToken(TokenType.KEYWORD, "WHERE", 0, depth))
    result.extend(acl_tokens)
    return result


def _inject_acl_into_union(
    tokens: List[CypherToken],
    union_indices: List[int],
    node_clause: str,
) -> List[CypherToken]:
    boundaries = [0] + union_indices
    segments: List[List[CypherToken]] = []

    for seg_idx, start in enumerate(boundaries):
        clause_start = start if seg_idx == 0 else _skip_union_keyword(tokens, start)
        clause_end = boundaries[seg_idx + 1] if seg_idx + 1 < len(boundaries) else len(tokens)
        injected = _inject_acl_into_clause(tokens[clause_start:clause_end], node_clause)

        if seg_idx > 0:
            segments.append(tokens[start:clause_start])
        segments.append(injected)

    flat: List[CypherToken] = []
    for seg in segments:
        flat.extend(seg)
    return flat


def _skip_union_keyword(tokens: List[CypherToken], union_idx: int) -> int:
    idx = union_idx + 1
    while idx < len(tokens):
        tok = tokens[idx]
        if tok.token_type == TokenType.WHITESPACE:
            idx += 1
            continue
        if tok.token_type == TokenType.KEYWORD and tok.value.upper() == "ALL":
            idx += 1
            continue
        break
    return idx


class CypherPermissionFilter:
    def __init__(
        self,
        principal: SecurityPrincipal,
        default_deny_untagged: bool = True,
    ) -> None:
        self._principal = principal
        self._deny_untagged = default_deny_untagged

    def node_filter(self, alias: str) -> Tuple[str, Dict[str, str]]:
        if self._principal.is_admin:
            return "", {}

        clauses = []
        params: Dict[str, str] = {}

        if self._principal.team != "*":
            if self._deny_untagged:
                clauses.append(f"({alias}.team_owner = $acl_team)")
            else:
                clauses.append(
                    f"({alias}.team_owner = $acl_team "
                    f"OR {alias}.team_owner IS NULL)"
                )
            params["acl_team"] = self._principal.team

        if self._principal.namespace != "*":
            if self._deny_untagged:
                clauses.append(
                    f"($acl_namespace IN {alias}.namespace_acl)"
                )
            else:
                clauses.append(
                    f"($acl_namespace IN {alias}.namespace_acl "
                    f"OR {alias}.namespace_acl IS NULL)"
                )
            params["acl_namespace"] = self._principal.namespace

        if not clauses:
            if self._deny_untagged:
                clauses.append(f"({alias}.team_owner = $acl_team)")
            else:
                clauses.append(
                    f"({alias}.team_owner = $acl_team "
                    f"OR {alias}.team_owner IS NULL)"
                )
            params["acl_team"] = "public"

        return " AND ".join(clauses), params

    def edge_filter(
        self, target_alias: str
    ) -> Tuple[str, Dict[str, str]]:
        if self._principal.is_admin:
            return "", {}

        clause, params = self.node_filter(target_alias)
        return clause, params

    def inject_into_cypher(
        self, cypher: str, alias: str = "n"
    ) -> Tuple[str, Dict[str, str]]:
        if self._principal.is_admin:
            return cypher, {}

        node_clause, params = self.node_filter(alias)
        if not node_clause:
            return cypher, {}

        tokens = tokenize_cypher(cypher)
        union_indices = find_union_boundaries(tokens)

        if not union_indices:
            result_tokens = _inject_acl_into_clause(tokens, node_clause)
        else:
            result_tokens = _inject_acl_into_union(tokens, union_indices, node_clause)

        injected_cypher = reconstruct_cypher(result_tokens)
        try:
            validate_call_subquery_acl(injected_cypher)
        except UnfilteredCallMatchError as exc:
            _acl_logger.warning("CALL subquery ACL gap detected: %s", exc)
        return injected_cypher, params
