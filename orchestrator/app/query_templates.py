from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class QueryTemplate:
    name: str
    cypher: str
    parameters: Tuple[str, ...]
    description: str = ""


@dataclass(frozen=True)
class TemplateMatch:
    template_name: str
    params: Dict[str, str]


_SERVICE_NAME_PATTERN = re.compile(
    r"(?:of|if|for|from|does|about)\s+(?:the\s+)?"
    r"([a-zA-Z][\w-]*(?:-[a-zA-Z][\w-]*)*)(?:\s+(?:service|svc))?"
    r"|(?:is)\s+([a-zA-Z][\w-]*-[a-zA-Z][\w-]*)(?:\s+(?:service|svc))?",
    re.IGNORECASE,
)

_TOPIC_NAME_PATTERN = re.compile(
    r"(?:from|to|on|the)\s+(?:the\s+)?([a-zA-Z][\w-]*(?:-[a-zA-Z][\w-]*)*)(?:\s*(?:topic|queue))?",
    re.IGNORECASE,
)


_TEMPLATES: Dict[str, QueryTemplate] = {
    "blast_radius": QueryTemplate(
        name="blast_radius",
        cypher=(
            "MATCH (s:Service {name: $name, tenant_id: $tenant_id})"
            "-[r:CALLS|PRODUCES|CONSUMES*1..3]->(downstream) "
            "WHERE downstream.tenant_id = $tenant_id "
            "AND ALL(rel IN r WHERE rel.tombstoned_at IS NULL) "
            "RETURN DISTINCT downstream.name AS affected_service, "
            "labels(downstream)[0] AS node_type "
            "ORDER BY affected_service"
        ),
        parameters=("name", "tenant_id"),
        description="Transitive downstream blast radius from a service failure",
    ),
    "dependency_count": QueryTemplate(
        name="dependency_count",
        cypher=(
            "MATCH (caller:Service)-[r:CALLS]->(target:Service) "
            "WHERE caller.tenant_id = $tenant_id "
            "AND target.tenant_id = $tenant_id "
            "AND r.tombstoned_at IS NULL "
            "RETURN target.name AS service, count(caller) AS inbound_dependency_count "
            "ORDER BY inbound_dependency_count DESC "
            "LIMIT $limit"
        ),
        parameters=("limit", "tenant_id"),
        description="Services ranked by inbound dependency count",
    ),
    "service_neighbors": QueryTemplate(
        name="service_neighbors",
        cypher=(
            "MATCH (s:Service {name: $name, tenant_id: $tenant_id})-[r]-(neighbor) "
            "WHERE neighbor.tenant_id = $tenant_id "
            "AND r.tombstoned_at IS NULL "
            "RETURN s.name AS source, type(r) AS relationship, "
            "neighbor.name AS target, labels(neighbor)[0] AS target_type "
            "ORDER BY relationship, target"
        ),
        parameters=("name", "tenant_id"),
        description="All direct neighbors of a service",
    ),
    "topic_consumers": QueryTemplate(
        name="topic_consumers",
        cypher=(
            "MATCH (consumer:Service)-[r:CONSUMES]->(t:KafkaTopic {name: $topic_name}) "
            "WHERE consumer.tenant_id = $tenant_id "
            "AND t.tenant_id = $tenant_id "
            "AND r.tombstoned_at IS NULL "
            "RETURN consumer.name AS consumer_service, t.name AS topic "
            "ORDER BY consumer_service"
        ),
        parameters=("topic_name", "tenant_id"),
        description="Services consuming from a Kafka topic",
    ),
    "topic_producers": QueryTemplate(
        name="topic_producers",
        cypher=(
            "MATCH (producer:Service)-[r:PRODUCES]->(t:KafkaTopic {name: $topic_name}) "
            "WHERE producer.tenant_id = $tenant_id "
            "AND t.tenant_id = $tenant_id "
            "AND r.tombstoned_at IS NULL "
            "RETURN producer.name AS producer_service, t.name AS topic "
            "ORDER BY producer_service"
        ),
        parameters=("topic_name", "tenant_id"),
        description="Services producing to a Kafka topic",
    ),
    "service_deployments": QueryTemplate(
        name="service_deployments",
        cypher=(
            "MATCH (s:Service {name: $name, tenant_id: $tenant_id})"
            "-[r:DEPLOYED_IN]->(d:K8sDeployment) "
            "WHERE d.tenant_id = $tenant_id "
            "AND r.tombstoned_at IS NULL "
            "RETURN s.name AS service, d.namespace AS namespace, "
            "d.replicas AS replicas "
            "ORDER BY namespace"
        ),
        parameters=("name", "tenant_id"),
        description="K8s deployments hosting a service",
    ),
    "namespace_services": QueryTemplate(
        name="namespace_services",
        cypher=(
            "MATCH (s:Service)-[r:DEPLOYED_IN]->(d:K8sDeployment {namespace: $namespace}) "
            "WHERE s.tenant_id = $tenant_id "
            "AND d.tenant_id = $tenant_id "
            "AND r.tombstoned_at IS NULL "
            "RETURN DISTINCT s.name AS service, d.namespace AS namespace "
            "ORDER BY service"
        ),
        parameters=("namespace", "tenant_id"),
        description="All services deployed in a K8s namespace",
    ),
    "service_databases": QueryTemplate(
        name="service_databases",
        cypher=(
            "MATCH (s:Service {name: $name, tenant_id: $tenant_id})"
            "-[r:WRITES_TO|READS_FROM]->(db:Database) "
            "WHERE db.tenant_id = $tenant_id "
            "AND r.tombstoned_at IS NULL "
            "RETURN s.name AS service, db.id AS database, db.type AS db_type "
            "ORDER BY database"
        ),
        parameters=("name", "tenant_id"),
        description="Databases used by a service",
    ),
    "cross_team_dependencies": QueryTemplate(
        name="cross_team_dependencies",
        cypher=(
            "MATCH (a:Service)-[r:CALLS]->(b:Service) "
            "WHERE a.tenant_id = $tenant_id "
            "AND b.tenant_id = $tenant_id "
            "AND a.team_owner <> b.team_owner "
            "AND r.tombstoned_at IS NULL "
            "RETURN a.name AS caller, a.team_owner AS caller_team, "
            "b.name AS callee, b.team_owner AS callee_team "
            "ORDER BY caller_team, callee_team"
        ),
        parameters=("tenant_id",),
        description="Service calls that cross team boundaries",
    ),
}

_NAMESPACE_PATTERN = re.compile(
    r"(?:in|within|namespace)\s+(?:the\s+)?([a-zA-Z][\w-]*)",
    re.IGNORECASE,
)

_INTENT_PATTERNS: List[Tuple[re.Pattern[str], str]] = [
    (re.compile(
        r"blast\s*radius|downstream.*fail|impact.*fail|fail.*impact",
        re.IGNORECASE,
    ), "blast_radius"),
    (re.compile(
        r"dependency\s*count|most\s*critical|most\s*depended"
        r"|ranked\s*by.*dep",
        re.IGNORECASE,
    ), "dependency_count"),
    (re.compile(
        r"(?:what|who)\s+does\s+\S+\s+call|neighbors?\s+of"
        r"|connected\s+to|calls?\s+from",
        re.IGNORECASE,
    ), "service_neighbors"),
    (re.compile(
        r"consum(?:e|es|ers?|ing)\s+(?:from|the)"
        r"|subscribers?\s+(?:of|to|for)",
        re.IGNORECASE,
    ), "topic_consumers"),
    (re.compile(
        r"produc(?:e|es|ers?|ing)\s+(?:to|on|the)"
        r"|publish(?:es|ers?|ing)\s+(?:to|on|the)",
        re.IGNORECASE,
    ), "topic_producers"),
    (re.compile(
        r"deploy(?:ed|ments?)\s+(?:of|for|hosting)"
        r"|(?:where|how)\s+is\s+\S+\s+deployed"
        r"|k8s.*(?:pods?|replicas?)\s+(?:of|for)"
        r"|\bdeployed\b",
        re.IGNORECASE,
    ), "service_deployments"),
    (re.compile(
        r"services?\s+(?:in|within|deployed\s+in)\s+(?:the\s+)?(?:namespace|ns)"
        r"|namespace\s+\S+\s+services?",
        re.IGNORECASE,
    ), "namespace_services"),
    (re.compile(
        r"databas(?:e|es)\s+(?:used|accessed|of|for)\s+"
        r"|(?:reads?|writes?)\s+(?:to|from)\s+(?:which|what)\s+databas",
        re.IGNORECASE,
    ), "service_databases"),
    (re.compile(
        r"cross[\s-]?team\s+dep|inter[\s-]?team\s+call"
        r"|calls?\s+across\s+teams?",
        re.IGNORECASE,
    ), "cross_team_dependencies"),
]


ALLOWED_RELATIONSHIP_TYPES = frozenset({
    "CALLS", "PRODUCES", "CONSUMES", "DEPLOYED_IN",
})

_ACL_NODE_FILTER = (
    "({alias}.tenant_id = $tenant_id) "
    "AND ($is_admin OR {alias}.team_owner = $acl_team "
    "OR ANY(ns IN {alias}.namespace_acl WHERE ns IN $acl_namespaces))"
)

_ACL_TEMPLATES: Dict[str, QueryTemplate] = {
    "acl_vector_search": QueryTemplate(
        name="acl_vector_search",
        cypher=(
            "CALL db.index.vector.queryNodes($index_name, $k, $query_embedding) "
            "YIELD node, score "
            "WHERE node.tenant_id = $tenant_id "
            "AND ($is_admin OR node.team_owner = $acl_team "
            "OR ANY(ns IN node.namespace_acl WHERE ns IN $acl_namespaces)) "
            "RETURN node {.*, score: score} AS result "
            "ORDER BY score DESC "
            "LIMIT $limit"
        ),
        parameters=("index_name", "k", "query_embedding", "tenant_id",
                     "is_admin", "acl_team", "acl_namespaces", "limit"),
        description="Vector search with mandatory tenant isolation and ACL filtering",
    ),
    "acl_fulltext_search": QueryTemplate(
        name="acl_fulltext_search",
        cypher=(
            "CALL db.index.fulltext.queryNodes('service_name_index', $query) "
            "YIELD node, score "
            "WHERE node.tenant_id = $tenant_id "
            "AND ($is_admin OR node.team_owner = $acl_team "
            "OR ANY(ns IN node.namespace_acl WHERE ns IN $acl_namespaces)) "
            "RETURN node {.*, score: score} AS result "
            "ORDER BY score DESC LIMIT $limit"
        ),
        parameters=("query", "tenant_id", "is_admin", "acl_team",
                     "acl_namespaces", "limit"),
        description="Fulltext search with mandatory tenant isolation and ACL filtering",
    ),
}


def build_acl_single_hop_query(rel_type: str) -> str:
    if rel_type not in ALLOWED_RELATIONSHIP_TYPES:
        raise ValueError(f"Disallowed relationship type: {rel_type}")
    return (
        f"MATCH (source:Service {{id: $source_id, tenant_id: $tenant_id}}) "
        f"WHERE $is_admin OR source.team_owner = $acl_team "
        f"OR ANY(ns IN source.namespace_acl WHERE ns IN $acl_namespaces) "
        f"MATCH (source)-[r:{rel_type}]->(target) "
        f"WHERE target.tenant_id = $tenant_id "
        f"AND r.tombstoned_at IS NULL "
        f"AND ($is_admin OR target.team_owner = $acl_team "
        f"OR ANY(ns IN target.namespace_acl WHERE ns IN $acl_namespaces)) "
        f"RETURN target {{.*}} AS result "
        f"LIMIT $limit"
    )


def build_acl_multi_hop_query(max_depth: int) -> str:
    max_depth = min(int(max_depth), 5)
    return (
        f"MATCH path = (source:Service {{id: $source_id, tenant_id: $tenant_id}})"
        f"-[:CALLS*1..{max_depth}]->(target:Service) "
        f"WHERE ALL(n IN nodes(path) WHERE "
        f"n.tenant_id = $tenant_id "
        f"AND ($is_admin OR n.team_owner = $acl_team "
        f"OR ANY(ns IN n.namespace_acl WHERE ns IN $acl_namespaces))) "
        f"AND ALL(rel IN relationships(path) WHERE rel.tombstoned_at IS NULL) "
        f"RETURN target.name AS name, length(path) AS hops "
        f"ORDER BY hops "
        f"LIMIT $limit"
    )


def dynamic_cypher_allowed() -> bool:
    return os.environ.get("ALLOW_DYNAMIC_CYPHER", "false").lower() in (
        "true", "1", "yes",
    )


def build_acl_params(
    tenant_id: str,
    is_admin: bool,
    team: str,
    namespaces: List[str],
) -> Dict[str, Any]:
    return {
        "tenant_id": tenant_id,
        "is_admin": is_admin,
        "acl_team": team,
        "acl_namespaces": namespaces,
    }


class TemplateCatalog:
    def __init__(self) -> None:
        self._templates: Dict[str, QueryTemplate] = {**_TEMPLATES, **_ACL_TEMPLATES}

    def get(self, name: str) -> Optional[QueryTemplate]:
        return self._templates.get(name)

    def all_templates(self) -> Dict[str, QueryTemplate]:
        return dict(self._templates)

    def get_acl_template(self, name: str) -> Optional[QueryTemplate]:
        return _ACL_TEMPLATES.get(name)


def _extract_service_name(query: str) -> str:
    match = _SERVICE_NAME_PATTERN.search(query)
    if match:
        return match.group(1) or match.group(2) or ""
    return ""


def _extract_topic_name(query: str) -> str:
    match = _TOPIC_NAME_PATTERN.search(query)
    if match:
        return match.group(1)
    return ""


def _extract_namespace(query: str) -> str:
    match = _NAMESPACE_PATTERN.search(query)
    if match:
        return match.group(1)
    return ""


def match_template(query: str) -> Optional[TemplateMatch]:
    matched_intent: Optional[str] = None
    for pattern, intent in _INTENT_PATTERNS:
        if pattern.search(query):
            matched_intent = intent
            break

    if matched_intent is None:
        return None

    template = _TEMPLATES.get(matched_intent)
    if template is None:
        return None

    params: Dict[str, str] = {}
    if "name" in template.parameters:
        service_name = _extract_service_name(query)
        if service_name:
            params["name"] = service_name
    if "topic_name" in template.parameters:
        topic_name = _extract_topic_name(query)
        if topic_name:
            params["topic_name"] = topic_name
    if "namespace" in template.parameters:
        ns = _extract_namespace(query)
        if ns:
            params["namespace"] = ns
    if "limit" in template.parameters:
        params["limit"] = "10"

    non_context_params = [p for p in template.parameters if p != "tenant_id"]
    if any(p not in params for p in non_context_params):
        return None

    return TemplateMatch(template_name=matched_intent, params=params)
