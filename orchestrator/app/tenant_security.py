from __future__ import annotations

import logging
import re
from typing import Any, Dict, FrozenSet, List

from orchestrator.app.cypher_ast import validate_acl_coverage
from orchestrator.app.query_templates import ALLOWED_RELATIONSHIP_TYPES, TemplateCatalog

_logger = logging.getLogger(__name__)

_TENANT_SCOPE_PATTERN = re.compile(
    r"\$tenant_id|\$tid|tenant_id\s*[:=]|tenant_id\b",
)


class SecurityViolationError(Exception):
    pass


class TenantTemplateViolationError(Exception):
    def __init__(self, unscoped_names: List[str]) -> None:
        self.unscoped_names = unscoped_names
        joined = ", ".join(sorted(unscoped_names))
        super().__init__(
            f"Query templates missing tenant_id scope: [{joined}]. "
            f"Every Cypher template must include a tenant_id parameter to "
            f"prevent cross-tenant data leakage."
        )


class TenantScopeVerifier:
    def __init__(
        self,
        catalog: TemplateCatalog,
        exempt_templates: FrozenSet[str] = frozenset(),
    ) -> None:
        self._catalog = catalog
        self._exempt_templates = exempt_templates

    @property
    def exempt_templates(self) -> FrozenSet[str]:
        return self._exempt_templates

    def find_unscoped_templates(self) -> List[str]:
        violations: List[str] = []
        for name, template in self._catalog.all_templates().items():
            if name in self._exempt_templates:
                continue
            if not self.has_tenant_scope(template.cypher):
                violations.append(name)
        return sorted(violations)

    def verify_all_templates(self) -> None:
        violations = self.find_unscoped_templates()
        if violations:
            raise TenantTemplateViolationError(violations)

    def enforce_startup(self, deployment_mode: str) -> None:
        violations = self.find_unscoped_templates()
        if not violations:
            return
        if deployment_mode == "production":
            raise TenantTemplateViolationError(violations)
        joined = ", ".join(violations)
        _logger.warning(
            "Tenant scope verification warning: templates [%s] are missing "
            "tenant_id scope. This would be fatal in production mode.",
            joined,
        )

    @staticmethod
    def has_tenant_scope(cypher: str) -> bool:
        return bool(_TENANT_SCOPE_PATTERN.search(cypher))


_ACL_MARKERS: FrozenSet[str] = frozenset({
    "$is_admin", "$acl_team", "$acl_namespaces",
    "team_owner", "namespace_acl",
})


class TenantSecurityProvider:

    def validate_query(
        self,
        cypher: str,
        params: Dict[str, Any],
        require_acl: bool = True,
    ) -> None:
        tenant_id = params.get("tenant_id")
        if not tenant_id:
            raise SecurityViolationError(
                "tenant_id parameter is missing or empty"
            )

        if "tenant_id" not in cypher:
            raise SecurityViolationError(
                "query does not reference tenant_id"
            )

        if not validate_acl_coverage(cypher, "tenant_id"):
            raise SecurityViolationError(
                "tenant_id filter missing from one or more MATCH scopes"
            )

        if require_acl and not any(marker in cypher for marker in _ACL_MARKERS):
            raise SecurityViolationError(
                "query does not contain ACL enforcement clause"
            )


def _acl_where_fragment() -> str:
    return (
        "AND ($is_admin OR target.team_owner = $acl_team "
        "OR ANY(ns IN target.namespace_acl WHERE ns IN $acl_namespaces)) "
    )


def build_traversal_one_hop(rel_type: str) -> str:
    if rel_type not in ALLOWED_RELATIONSHIP_TYPES:
        raise ValueError(f"Disallowed relationship type: {rel_type}")
    acl = _acl_where_fragment()
    return (
        "MATCH (source {id: $source_id, tenant_id: $tenant_id})"
        f"-[r:{rel_type}]->(target) "
        "WHERE target.tenant_id = $tenant_id "
        "AND r.tombstoned_at IS NULL "
        f"{acl}"
        "RETURN target {.*} AS result, type(r) AS rel_type "
        "LIMIT $limit"
    )


def build_traversal_neighbor_discovery() -> str:
    acl = _acl_where_fragment()
    return (
        "MATCH (source {id: $source_id, tenant_id: $tenant_id})"
        "-[r]->(target) "
        "WHERE target.tenant_id = $tenant_id "
        "AND r.tombstoned_at IS NULL "
        f"{acl}"
        "RETURN target.id AS target_id, target.name AS target_name, "
        "type(r) AS rel_type, labels(target)[0] AS target_label "
        "ORDER BY coalesce(target.pagerank, 0) DESC, "
        "coalesce(target.degree, 0) DESC, target.id "
        "LIMIT $limit"
    )


def build_traversal_sampled_neighbor() -> str:
    acl = _acl_where_fragment()
    return (
        "MATCH (source {id: $source_id, tenant_id: $tenant_id})"
        "-[r]->(target) "
        "WHERE target.tenant_id = $tenant_id AND r.tombstoned_at IS NULL "
        f"{acl}"
        "RETURN target.id AS target_id, target.name AS target_name, "
        "type(r) AS rel_type, labels(target)[0] AS target_label "
        "ORDER BY coalesce(target.pagerank, 0) DESC, "
        "coalesce(target.degree, 0) DESC, target.id "
        "LIMIT $sample_size"
    )


def build_traversal_batched_neighbor() -> str:
    acl = _acl_where_fragment()
    return (
        "UNWIND $frontier_ids AS fid "
        "MATCH (source {id: fid, tenant_id: $tenant_id})"
        "-[r]->(target) "
        "WHERE target.tenant_id = $tenant_id "
        "AND r.tombstoned_at IS NULL "
        f"{acl}"
        "WITH source, target, r, "
        "coalesce(target.pagerank, 0) AS pagerank, "
        "coalesce(target.degree, 0) AS degree "
        "ORDER BY pagerank DESC, degree DESC "
        "WITH source, collect({target_id: target.id, "
        "target_name: target.name, rel_type: type(r), "
        "target_label: labels(target)[0], "
        "pagerank: pagerank, degree: degree})"
        "[0..$per_source_limit] AS neighbors "
        "UNWIND neighbors AS n "
        "RETURN source.id AS source_id, n.target_id AS target_id, "
        "n.target_name AS target_name, n.rel_type AS rel_type, "
        "n.target_label AS target_label, "
        "n.pagerank AS pagerank, n.degree AS degree "
        "LIMIT $limit"
    )


def build_traversal_batched_supernode_neighbor() -> str:
    acl = _acl_where_fragment()
    return (
        "UNWIND $source_ids AS sid "
        "MATCH (source {id: sid, tenant_id: $tenant_id})"
        "-[r]->(target) "
        "WHERE target.tenant_id = $tenant_id "
        "AND r.tombstoned_at IS NULL "
        f"{acl}"
        "WITH source, target, r, "
        "coalesce(target.pagerank, 0) AS pagerank, "
        "coalesce(target.degree, 0) AS degree "
        "ORDER BY pagerank DESC, degree DESC "
        "WITH source, collect({target_id: target.id, "
        "target_name: target.name, rel_type: type(r), "
        "target_label: labels(target)[0], "
        "pagerank: pagerank, degree: degree})"
        "[0..$sample_size] AS neighbors "
        "UNWIND neighbors AS n "
        "RETURN source.id AS source_id, n.target_id AS target_id, "
        "n.target_name AS target_name, n.rel_type AS rel_type, "
        "n.target_label AS target_label, "
        "n.pagerank AS pagerank, n.degree AS degree"
    )
