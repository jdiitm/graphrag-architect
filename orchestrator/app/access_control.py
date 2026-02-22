from __future__ import annotations

import hashlib
import hmac
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

DEFAULT_TOKEN_TTL_SECONDS = 3600


class InvalidTokenError(Exception):
    pass


class AuthConfigurationError(Exception):
    pass


def sign_token(
    payload: str,
    secret: str,
    ttl_seconds: int = DEFAULT_TOKEN_TTL_SECONDS,
) -> str:
    now = int(time.time())
    payload_with_claims = f"{payload},iat={now},exp={now + ttl_seconds}"
    signature = hmac.new(
        secret.encode(), payload_with_claims.encode(), hashlib.sha256
    ).hexdigest()
    return f"{payload_with_claims}.{signature}"


def _verify_signature(token: str, secret: str) -> str:
    dot_pos = token.rfind(".")
    if dot_pos < 0:
        raise InvalidTokenError("token missing signature")
    payload = token[:dot_pos]
    provided_sig = token[dot_pos + 1:]
    expected_sig = hmac.new(
        secret.encode(), payload.encode(), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(provided_sig, expected_sig):
        raise InvalidTokenError("token signature invalid")
    _check_expiration(payload)
    return payload


def _check_expiration(payload: str) -> None:
    fields: Dict[str, str] = {}
    for pair in payload.split(","):
        if "=" in pair:
            key, value = pair.split("=", 1)
            fields[key.strip()] = value.strip()
    exp_str = fields.get("exp")
    if exp_str is None:
        raise InvalidTokenError("token missing expiration")
    try:
        exp = int(exp_str)
    except ValueError as exc:
        raise InvalidTokenError("token expiration is not a valid integer") from exc
    if time.time() >= exp:
        raise InvalidTokenError("token expired")


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
    ) -> SecurityPrincipal:
        if not header or not header.strip():
            return cls(team="*", namespace="*", role="anonymous")

        token = header.removeprefix("Bearer ").strip()

        if token_secret:
            payload = _verify_signature(token, token_secret)
        else:
            payload = token

        fields: Dict[str, str] = {}
        for pair in payload.split(","):
            if "=" in pair:
                key, value = pair.split("=", 1)
                fields[key.strip()] = value.strip()

        return cls(
            team=fields.get("team", "*"),
            namespace=fields.get("namespace", "*"),
            role=fields.get("role", "viewer"),
        )


def _find_toplevel_keywords(cypher: str) -> Tuple[int, int, int]:
    length = len(cypher)
    brace_depth = 0
    in_quote: Optional[str] = None
    top_where_start = -1
    top_where_end = -1
    top_return_start = -1
    idx = 0

    while idx < length:
        char = cypher[idx]

        if in_quote is not None:
            if char == "\\" and idx + 1 < length:
                idx += 2
                continue
            if char == in_quote:
                in_quote = None
            idx += 1
            continue

        if char in ("'", '"'):
            in_quote = char
            idx += 1
            continue

        if char == "{":
            brace_depth += 1
            idx += 1
            continue

        if char == "}":
            brace_depth = max(0, brace_depth - 1)
            idx += 1
            continue

        if brace_depth > 0:
            idx += 1
            continue

        if char in ("W", "w") and top_where_start < 0:
            candidate = cypher[idx:idx + 5]
            if candidate.upper() == "WHERE" and _is_word_boundary(cypher, idx, 5):
                top_where_start = idx
                top_where_end = idx + 5
                idx += 5
                continue

        if char in ("R", "r") and top_return_start < 0:
            candidate = cypher[idx:idx + 6]
            if candidate.upper() == "RETURN" and _is_word_boundary(cypher, idx, 6):
                top_return_start = idx
                idx += 6
                continue

        idx += 1

    return top_where_start, top_where_end, top_return_start


def _is_word_boundary(text: str, start: int, length: int) -> bool:
    if start > 0 and text[start - 1].isalnum():
        return False
    end = start + length
    if end < len(text) and text[end].isalnum():
        return False
    return True


class CypherPermissionFilter:
    def __init__(self, principal: SecurityPrincipal) -> None:
        self._principal = principal

    def node_filter(self, alias: str) -> Tuple[str, Dict[str, str]]:
        if self._principal.is_admin:
            return "", {}

        clauses = []
        params: Dict[str, str] = {}

        if self._principal.team != "*":
            clauses.append(
                f"({alias}.team_owner = $acl_team OR {alias}.team_owner IS NULL)"
            )
            params["acl_team"] = self._principal.team

        if self._principal.namespace != "*":
            clauses.append(
                f"($acl_namespace IN {alias}.namespace_acl "
                f"OR {alias}.namespace_acl IS NULL)"
            )
            params["acl_namespace"] = self._principal.namespace

        if not clauses:
            clauses.append(
                f"({alias}.team_owner = $acl_team OR {alias}.team_owner IS NULL)"
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

        where_pos, where_end, return_pos = _find_toplevel_keywords(cypher)

        if where_pos >= 0:
            if return_pos > where_end:
                existing_conds = cypher[where_end:return_pos].strip()
                filtered = (
                    cypher[:where_end]
                    + " " + node_clause
                    + " AND (" + existing_conds + ") "
                    + cypher[return_pos:]
                )
            else:
                existing_conds = cypher[where_end:].strip()
                filtered = (
                    cypher[:where_end]
                    + " " + node_clause
                    + " AND (" + existing_conds + ")"
                )
        elif return_pos >= 0:
            filtered = (
                cypher[:return_pos]
                + "WHERE " + node_clause + " "
                + cypher[return_pos:]
            )
        else:
            filtered = cypher + " WHERE " + node_clause

        return filtered, params
