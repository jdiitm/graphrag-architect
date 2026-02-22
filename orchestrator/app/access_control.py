from __future__ import annotations

import hashlib
import hmac
import re
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

DEFAULT_TOKEN_TTL_SECONDS = 3600


class InvalidTokenError(Exception):
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
        """Inject ACL WHERE clauses into a Cypher query string.

        Limitations: Uses regex-based keyword detection (``\\bWHERE\\b``,
        ``\\bRETURN\\b``).  This will mis-identify keywords inside string
        literals, CASE expressions, or nested sub-queries.  Queries with
        those constructs should use ``node_filter`` directly and build
        the Cypher manually.
        """
        if self._principal.is_admin:
            return cypher, {}

        node_clause, params = self.node_filter(alias)
        if not node_clause:
            return cypher, {}

        where_match = re.search(r"\bWHERE\b", cypher, re.IGNORECASE)
        return_match = re.search(r"\bRETURN\b", cypher, re.IGNORECASE)

        if where_match:
            where_end = where_match.end()
            if return_match and return_match.start() > where_end:
                existing_conds = cypher[where_end:return_match.start()].strip()
                filtered = (
                    cypher[:where_end]
                    + " " + node_clause
                    + " AND (" + existing_conds + ") "
                    + cypher[return_match.start():]
                )
            else:
                existing_conds = cypher[where_end:].strip()
                filtered = (
                    cypher[:where_end]
                    + " " + node_clause
                    + " AND (" + existing_conds + ")"
                )
        elif return_match:
            insert_pos = return_match.start()
            filtered = (
                cypher[:insert_pos]
                + "WHERE " + node_clause + " "
                + cypher[insert_pos:]
            )
        else:
            filtered = cypher + " WHERE " + node_clause

        return filtered, params
