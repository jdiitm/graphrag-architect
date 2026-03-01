from __future__ import annotations

import ast
import pathlib
import re
from typing import Any, Dict, FrozenSet, Set


class TenantScopeViolationError(Exception):
    pass


_TENANT_ID_PATTERN = re.compile(r"\$tenant_id|tenant_id\s*[:=]")

_INDEX_PREFIX = re.compile(
    r"^\s*CREATE\s+(RANGE\s+|VECTOR\s+|FULLTEXT\s+|TEXT\s+|POINT\s+|LOOKUP\s+)?INDEX\b",
    re.IGNORECASE,
)
_CONSTRAINT_PREFIX = re.compile(
    r"^\s*CREATE\s+CONSTRAINT\b",
    re.IGNORECASE,
)
_SCHEMA_CALL_PREFIX = re.compile(
    r"^\s*CALL\s+(db\.|dbms\.|gds\.)",
    re.IGNORECASE,
)
_DROP_PREFIX = re.compile(
    r"^\s*DROP\s+(INDEX|CONSTRAINT)\b",
    re.IGNORECASE,
)

SCHEMA_DDL_ALLOWLIST: FrozenSet[str] = frozenset({
    "CREATE RANGE INDEX tombstone_calls_idx IF NOT EXISTS "
    "FOR ()-[r:CALLS]-() ON (r.tombstoned_at)",
    "CREATE RANGE INDEX tombstone_produces_idx IF NOT EXISTS "
    "FOR ()-[r:PRODUCES]-() ON (r.tombstoned_at)",
    "CREATE RANGE INDEX tombstone_consumes_idx IF NOT EXISTS "
    "FOR ()-[r:CONSUMES]-() ON (r.tombstoned_at)",
    "CREATE RANGE INDEX tombstone_deployed_in_idx IF NOT EXISTS "
    "FOR ()-[r:DEPLOYED_IN]-() ON (r.tombstoned_at)",
    "CALL dbms.components() YIELD edition RETURN edition",
})

_INTERNAL_NODE_LABELS = frozenset({
    "OutboxEvent", "_SchemaPointer",
})

_INTERNAL_NODE_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(lbl) for lbl in _INTERNAL_NODE_LABELS) + r")\b",
)

_INTERPOLATED_MARKER = "$INTERPOLATED"

_ADMIN_MAINTENANCE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"tombstoned_at\s+IS\s+NOT\s+NULL.*DELETE\s+r", re.IGNORECASE),
    re.compile(r"tombstoned_at\s+IS\s+NOT\s+NULL.*RETURN\s+DISTINCT", re.IGNORECASE),
)


def _is_internal_infrastructure_query(query: str) -> bool:
    return bool(_INTERNAL_NODE_PATTERN.search(query))


def _is_interpolated_template(query: str) -> bool:
    return _INTERPOLATED_MARKER in query


def _is_query_fragment(query: str) -> bool:
    stripped = query.rstrip()
    return stripped.endswith((":","(",",","["))


def _is_admin_maintenance_query(query: str) -> bool:
    return any(p.search(query) for p in _ADMIN_MAINTENANCE_PATTERNS)


def _is_schema_ddl(query: str) -> bool:
    stripped = query.strip()
    if _INDEX_PREFIX.match(stripped):
        return True
    if _CONSTRAINT_PREFIX.match(stripped):
        return True
    if _SCHEMA_CALL_PREFIX.match(stripped):
        return True
    if _DROP_PREFIX.match(stripped):
        return True
    return False


def _query_references_tenant_id(query: str) -> bool:
    return bool(_TENANT_ID_PATTERN.search(query))


class TenantScopedSession:
    def __init__(
        self,
        tenant_id: str,
        allowlist: FrozenSet[str] | None = None,
    ) -> None:
        self._tenant_id = tenant_id
        self._allowlist = allowlist or SCHEMA_DDL_ALLOWLIST

    @property
    def tenant_id(self) -> str:
        return self._tenant_id

    def validate_query(
        self,
        query: str,
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        if query in self._allowlist or _is_schema_ddl(query):
            return dict(params)

        if not _query_references_tenant_id(query):
            raise TenantScopeViolationError(
                "Cypher query does not reference $tenant_id and is not on "
                "the schema DDL allowlist. All data queries must include "
                "tenant_id scoping to prevent cross-tenant data leakage."
            )

        result = dict(params)
        existing_tenant = result.get("tenant_id")

        if existing_tenant is not None and existing_tenant != self._tenant_id:
            raise TenantScopeViolationError(
                f"tenant_id parameter mismatch: session bound to "
                f"{self._tenant_id!r} but query supplies "
                f"{existing_tenant!r}. Cross-tenant access blocked."
            )

        if existing_tenant is None:
            result["tenant_id"] = self._tenant_id

        return result


_CYPHER_STATEMENT_PATTERN = re.compile(
    r"(?:^|\s)(?:"
    r"MATCH\s*[\(\[]"
    r"|MERGE\s*[\(\[]"
    r"|UNWIND\s+\$"
    r"|CREATE\s+(?:RANGE|VECTOR|FULLTEXT|TEXT|POINT|LOOKUP|CONSTRAINT)"
    r"|CALL\s+(?:db\.|dbms\.|apoc\.|gds\.)"
    r")",
    re.IGNORECASE,
)


def _looks_like_cypher(value: str) -> bool:
    if len(value) < 15:
        return False
    return bool(_CYPHER_STATEMENT_PATTERN.search(value))


class CypherTenantGuard:
    def __init__(
        self,
        allowlist: FrozenSet[str] | None = None,
    ) -> None:
        self._allowlist = allowlist or SCHEMA_DDL_ALLOWLIST

    def scan_queries(self, queries: Set[str]) -> Set[str]:
        violations: Set[str] = set()
        for query in queries:
            if self._is_exempt(query):
                continue
            if not _query_references_tenant_id(query):
                violations.add(query)
        return violations

    def _is_exempt(self, query: str) -> bool:
        if query in self._allowlist or _is_schema_ddl(query):
            return True
        structural_exempt = (
            _is_internal_infrastructure_query(query)
            or _is_interpolated_template(query)
            or _is_query_fragment(query)
            or _is_admin_maintenance_query(query)
        )
        return structural_exempt

    def extract_cypher_constants_from_directory(
        self,
        directory: pathlib.Path,
    ) -> Set[str]:
        cypher_constants: Set[str] = set()
        for py_file in sorted(directory.rglob("*.py")):
            cypher_constants.update(
                self._extract_from_file(py_file),
            )
        return cypher_constants

    def _extract_from_file(
        self,
        filepath: pathlib.Path,
    ) -> Set[str]:
        source = filepath.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(filepath))
        constants: Set[str] = set()
        for node in ast.walk(tree):
            value = self._extract_string_value(node)
            if value is not None and _looks_like_cypher(value):
                constants.add(value)
        return constants

    @staticmethod
    def _extract_string_value(node: ast.AST) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr):
            parts = []
            for value in node.values:
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    parts.append(value.value)
                else:
                    parts.append("$INTERPOLATED")
            joined = "".join(parts)
            if _looks_like_cypher(joined):
                return joined
            return None
        return None
