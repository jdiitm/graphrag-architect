from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import jwt

from orchestrator.app.cypher_ast import inject_acl_all_scopes, validate_acl_coverage

_LABEL_UNSAFE = re.compile(r"[^a-zA-Z0-9_]")


def _sanitize_label_suffix(value: str) -> str:
    return _LABEL_UNSAFE.sub("_", value)


def compute_acl_labels(principal: SecurityPrincipal) -> List[str]:
    labels: List[str] = []
    if principal.team and principal.team != "*":
        labels.append(f"Team_{_sanitize_label_suffix(principal.team)}")
    if principal.namespace and principal.namespace != "*":
        labels.append(f"Ns_{_sanitize_label_suffix(principal.namespace)}")
    if principal.role and principal.role not in ("*", "anonymous", "admin"):
        labels.append(f"Role_{_sanitize_label_suffix(principal.role)}")
    return labels

DEFAULT_TOKEN_TTL_SECONDS = 3600
JWT_ALGORITHM = "HS256"


class InvalidTokenError(Exception):
    pass


class AuthConfigurationError(Exception):
    pass


class ACLCoverageError(Exception):
    pass


@dataclass(frozen=True)
class KeyRotationConfig:
    current_key: str
    previous_key: str
    overlap_window_seconds: int = 300


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


def verify_token_with_rotation(
    token: str, config: KeyRotationConfig
) -> Dict[str, Any]:
    try:
        return _verify_token(token, config.current_key)
    except InvalidTokenError:
        pass

    if config.previous_key:
        try:
            return _verify_token(token, config.previous_key)
        except InvalidTokenError:
            pass

    raise InvalidTokenError("token rejected by all rotation keys")


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
            return cls(team="__anonymous__", namespace="__anonymous__", role="anonymous")

        token = header.removeprefix("Bearer ").strip()

        if token_secret:
            claims = _verify_token(token, token_secret)
        else:
            raise InvalidTokenError(
                "token provided but no secret configured for verification"
            )

        return cls(
            team=claims.get("team", "__anonymous__"),
            namespace=claims.get("namespace", "__anonymous__"),
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

    def _label_acl_clause(self, alias: str) -> str:
        match_expr = f"ANY(lbl IN labels({alias}) WHERE lbl IN $acl_labels)"
        if self._deny_untagged:
            return match_expr
        return (
            f"({match_expr} OR NONE(lbl IN labels({alias}) "
            f"WHERE lbl STARTS WITH 'Team_' "
            f"OR lbl STARTS WITH 'Ns_' "
            f"OR lbl STARTS WITH 'Role_'))"
        )

    def node_filter(self, alias: str) -> Tuple[str, Dict[str, Any]]:
        if self._principal.is_admin:
            return "", {}

        acl_labels = compute_acl_labels(self._principal)
        if not acl_labels:
            acl_labels = ["Team_public"]

        clause = self._label_acl_clause(alias)
        return clause, {"acl_labels": acl_labels}

    def edge_filter(
        self, target_alias: str
    ) -> Tuple[str, Dict[str, Any]]:
        if self._principal.is_admin:
            return "", {}

        clause, params = self.node_filter(target_alias)
        return clause, params

    def inject_into_cypher(
        self, cypher: str, alias: str = "n"
    ) -> Tuple[str, Dict[str, Any]]:
        if self._principal.is_admin:
            return cypher, {}

        node_clause, params = self.node_filter(alias)
        if not node_clause:
            return cypher, {}

        injected_cypher = inject_acl_all_scopes(cypher, node_clause)
        if self._verify_coverage:
            if not validate_acl_coverage(injected_cypher, "acl_labels"):
                raise ACLCoverageError(
                    "ACL injection did not cover all MATCH scopes in the query"
                )
        return injected_cypher, params
