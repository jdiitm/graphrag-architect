from __future__ import annotations

from typing import Any, Dict, FrozenSet

from orchestrator.app.cypher_ast import validate_acl_coverage
from orchestrator.app.query_templates import ALLOWED_RELATIONSHIP_TYPES


class SecurityViolationError(Exception):
    pass


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
        "RETURN source.id AS source_id, target.id AS target_id, "
        "target.name AS target_name, type(r) AS rel_type, "
        "labels(target)[0] AS target_label, "
        "coalesce(target.pagerank, 0) AS pagerank, "
        "coalesce(target.degree, 0) AS degree "
        "ORDER BY pagerank DESC, degree DESC "
        "LIMIT $limit"
    )
