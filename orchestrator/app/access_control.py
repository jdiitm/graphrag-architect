from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import jwt

from orchestrator.app.cypher_ast import inject_acl_all_scopes, validate_acl_coverage

DEFAULT_TOKEN_TTL_SECONDS = 3600
JWT_ALGORITHM = "HS256"


class InvalidTokenError(Exception):
    pass


class AuthConfigurationError(Exception):
    pass


class ACLCoverageError(Exception):
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
    ) -> SecurityPrincipal:
        if not header or not header.strip():
            return cls(team="*", namespace="*", role="anonymous")

        token = header.removeprefix("Bearer ").strip()

        if token_secret:
            claims = _verify_token(token, token_secret)
        else:
            raise InvalidTokenError(
                "token provided but no secret configured for verification"
            )

        return cls(
            team=claims.get("team", "*"),
            namespace=claims.get("namespace", "*"),
            role=claims.get("role", "viewer"),
        )


class CypherPermissionFilter:
    def __init__(
        self,
        principal: SecurityPrincipal,
        default_deny_untagged: bool = True,
        verify_coverage: bool = True,
    ) -> None:
        self._principal = principal
        self._deny_untagged = default_deny_untagged
        self._verify_coverage = verify_coverage

    @property
    def verify_coverage(self) -> bool:
        return self._verify_coverage

    def _acl_clause(
        self, alias: str, field: str, param: str, use_in: bool = False,
    ) -> str:
        op = f"${param} IN {alias}.{field}" if use_in else f"{alias}.{field} = ${param}"
        if self._deny_untagged:
            return f"({op})"
        return f"({op} OR {alias}.{field} IS NULL)"

    def node_filter(self, alias: str) -> Tuple[str, Dict[str, str]]:
        if self._principal.is_admin:
            return "", {}

        clauses = []
        params: Dict[str, str] = {}

        if self._principal.team != "*":
            clauses.append(self._acl_clause(alias, "team_owner", "acl_team"))
            params["acl_team"] = self._principal.team

        if self._principal.namespace != "*":
            clauses.append(
                self._acl_clause(alias, "namespace_acl", "acl_namespace", use_in=True),
            )
            params["acl_namespace"] = self._principal.namespace

        if self._principal.role not in ("*", "anonymous", "admin"):
            clauses.append(
                self._acl_clause(alias, "read_roles", "acl_role", use_in=True),
            )
            params["acl_role"] = self._principal.role

        if not clauses:
            clauses.append(self._acl_clause(alias, "team_owner", "acl_team"))
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

        injected_cypher = inject_acl_all_scopes(cypher, node_clause)
        if self._verify_coverage:
            acl_marker = node_clause.split("=", maxsplit=1)[0].strip().split(".")[-1]
            if not validate_acl_coverage(injected_cypher, acl_marker):
                raise ACLCoverageError(
                    "ACL injection did not cover all MATCH scopes in the query"
                )
        return injected_cypher, params
