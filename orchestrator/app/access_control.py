from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass(frozen=True)
class SecurityPrincipal:
    team: str
    namespace: str
    role: str

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    @classmethod
    def from_header(cls, header: Optional[str]) -> SecurityPrincipal:
        if not header or not header.strip():
            return cls(team="*", namespace="*", role="anonymous")

        token = header.removeprefix("Bearer ").strip()
        fields: Dict[str, str] = {}
        for pair in token.split(","):
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
