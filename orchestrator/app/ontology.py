from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


@dataclass(frozen=True)
class NodeTypeDefinition:
    properties: Dict[str, str]
    unique_key: str
    merge_keys: List[str] = field(default_factory=list)
    acl_fields: List[str] = field(default_factory=list)

    def effective_merge_keys(self) -> List[str]:
        return self.merge_keys if self.merge_keys else [self.unique_key]


@dataclass(frozen=True)
class EdgeTypeDefinition:
    source_type: str
    target_type: str
    properties: Dict[str, str] = field(default_factory=dict)
    source_key: str = "id"
    target_key: str = "id"
    source_param: str = ""
    target_param: str = ""
    source_label: str = ""
    target_label: str = ""


@dataclass
class Ontology:
    node_types: Dict[str, NodeTypeDefinition] = field(default_factory=dict)
    edge_types: Dict[str, EdgeTypeDefinition] = field(default_factory=dict)

    def get_node_type(self, label: str) -> Optional[NodeTypeDefinition]:
        return self.node_types.get(label)

    def get_edge_type(self, rel_type: str) -> Optional[EdgeTypeDefinition]:
        return self.edge_types.get(rel_type)

    def all_node_labels(self) -> List[str]:
        return list(self.node_types.keys())

    def all_edge_types(self) -> List[str]:
        return list(self.edge_types.keys())


class OntologyLoader:
    @staticmethod
    def from_yaml_string(content: str) -> Ontology:
        data = yaml.safe_load(content)
        return OntologyLoader._build(data)

    @staticmethod
    def from_file(path: str) -> Ontology:
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        return OntologyLoader._build(data)

    @staticmethod
    def _build(data: Dict[str, Any]) -> Ontology:
        node_types: Dict[str, NodeTypeDefinition] = {}
        for label, definition in data.get("node_types", {}).items():
            node_types[label] = NodeTypeDefinition(
                properties=definition.get("properties", {}),
                unique_key=definition.get("unique_key", "id"),
                merge_keys=definition.get("merge_keys", []),
                acl_fields=definition.get("acl_fields", []),
            )

        edge_types: Dict[str, EdgeTypeDefinition] = {}
        for rel_type, definition in data.get("edge_types", {}).items():
            edge_types[rel_type] = EdgeTypeDefinition(
                source_type=definition.get("source_type", ""),
                target_type=definition.get("target_type", ""),
                properties=definition.get("properties", {}),
                source_key=definition.get("source_key", "id"),
                target_key=definition.get("target_key", "id"),
                source_param=definition.get("source_param", ""),
                target_param=definition.get("target_param", ""),
                source_label=definition.get("source_label", ""),
                target_label=definition.get("target_label", ""),
            )

        return Ontology(node_types=node_types, edge_types=edge_types)


def generate_merge_cypher(
    label: str, node_def: NodeTypeDefinition,
) -> str:
    keys = node_def.effective_merge_keys()
    merge_parts = [f"{k}: ${k}" for k in keys]
    merge_clause = ", ".join(merge_parts)
    set_parts = [
        f"n.{prop} = ${prop}"
        for prop in node_def.properties
        if prop not in keys
    ]
    set_clause = ", ".join(set_parts)
    return (
        f"MERGE (n:{label} {{{merge_clause}}}) "
        f"SET {set_clause}"
    )


def generate_unwind_cypher(
    label: str, node_def: NodeTypeDefinition,
) -> str:
    keys = node_def.effective_merge_keys()
    merge_parts = [f"{k}: row.{k}" for k in keys]
    merge_clause = ", ".join(merge_parts)
    set_parts = [
        f"n.{prop} = row.{prop}"
        for prop in node_def.properties
        if prop not in keys
    ]
    set_clause = ", ".join(set_parts)
    return (
        f"UNWIND $batch AS row "
        f"MERGE (n:{label} {{{merge_clause}}}) "
        f"SET {set_clause}"
    )


def generate_edge_merge_cypher(
    rel_type: str, edge_def: EdgeTypeDefinition,
) -> str:
    src_label = edge_def.source_label or edge_def.source_type
    tgt_label = edge_def.target_label or edge_def.target_type
    src_node_prop = edge_def.source_key
    tgt_node_prop = edge_def.target_key
    src_param = edge_def.source_param or edge_def.source_key
    tgt_param = edge_def.target_param or edge_def.target_key
    set_parts = [
        f"r.{prop} = ${prop}"
        for prop in edge_def.properties
    ]
    set_clause = ", ".join(set_parts)
    query = (
        f"MATCH (a:{src_label} {{{src_node_prop}: ${src_param}, tenant_id: $tenant_id}}), "
        f"(b:{tgt_label} {{{tgt_node_prop}: ${tgt_param}, tenant_id: $tenant_id}}) "
        f"MERGE (a)-[r:{rel_type}]->(b)"
    )
    if set_clause:
        query += f" SET {set_clause}"
    return query


def generate_edge_unwind_cypher(
    rel_type: str, edge_def: EdgeTypeDefinition,
) -> str:
    src_label = edge_def.source_label or edge_def.source_type
    tgt_label = edge_def.target_label or edge_def.target_type
    src_node_prop = edge_def.source_key
    tgt_node_prop = edge_def.target_key
    src_param = edge_def.source_param or edge_def.source_key
    tgt_param = edge_def.target_param or edge_def.target_key
    set_parts = [
        f"r.{prop} = row.{prop}"
        for prop in edge_def.properties
    ]
    set_clause = ", ".join(set_parts)
    query = (
        f"UNWIND $batch AS row "
        f"MATCH (a:{src_label} {{{src_node_prop}: row.{src_param}, tenant_id: row.tenant_id}}), "
        f"(b:{tgt_label} {{{tgt_node_prop}: row.{tgt_param}, tenant_id: row.tenant_id}}) "
        f"MERGE (a)-[r:{rel_type}]->(b)"
    )
    if set_clause:
        query += f" SET {set_clause}"
    return query


def _node_def(
    props: Dict[str, str],
    unique_key: str = "id",
    merge_keys: Optional[List[str]] = None,
    acl_fields: Optional[List[str]] = None,
) -> NodeTypeDefinition:
    return NodeTypeDefinition(
        properties=props,
        unique_key=unique_key,
        merge_keys=merge_keys or [],
        acl_fields=acl_fields or [],
    )


def _edge_def(
    src_type: str,
    tgt_type: str,
    props: Dict[str, str],
    src_key: str = "id",
    tgt_key: str = "id",
    src_param: str = "",
    tgt_param: str = "",
    src_label: str = "",
    tgt_label: str = "",
) -> EdgeTypeDefinition:
    return EdgeTypeDefinition(
        source_type=src_type,
        target_type=tgt_type,
        properties=props,
        source_key=src_key,
        target_key=tgt_key,
        source_param=src_param or src_key,
        target_param=tgt_param or tgt_key,
        source_label=src_label or src_type,
        target_label=tgt_label or tgt_type,
    )


_ACL_FIELDS = ["team_owner", "namespace_acl", "read_roles"]


def build_default_ontology() -> Ontology:
    node_types: Dict[str, NodeTypeDefinition] = {
        "Service": _node_def(
            {
                "id": "string", "name": "string", "language": "string",
                "framework": "string", "opentelemetry_enabled": "bool",
                "tenant_id": "string", "team_owner": "string",
                "namespace_acl": "list", "read_roles": "list",
                "confidence": "float",
            },
            unique_key="id",
            merge_keys=["id", "tenant_id"],
            acl_fields=_ACL_FIELDS,
        ),
        "Database": _node_def(
            {
                "id": "string", "type": "string",
                "tenant_id": "string", "team_owner": "string",
                "namespace_acl": "list", "read_roles": "list",
            },
            unique_key="id",
            merge_keys=["id", "tenant_id"],
            acl_fields=_ACL_FIELDS,
        ),
        "KafkaTopic": _node_def(
            {
                "name": "string", "partitions": "int",
                "retention_ms": "int", "tenant_id": "string",
                "team_owner": "string", "namespace_acl": "list",
                "read_roles": "list",
            },
            unique_key="name",
            merge_keys=["name", "tenant_id"],
            acl_fields=_ACL_FIELDS,
        ),
        "K8sDeployment": _node_def(
            {
                "id": "string", "namespace": "string",
                "replicas": "int", "tenant_id": "string",
                "team_owner": "string", "namespace_acl": "list",
                "read_roles": "list",
            },
            unique_key="id",
            merge_keys=["id", "tenant_id"],
            acl_fields=_ACL_FIELDS,
        ),
    }

    edge_types: Dict[str, EdgeTypeDefinition] = {
        "CALLS": _edge_def(
            "Service", "Service",
            {
                "protocol": "string", "confidence": "float",
                "ingestion_id": "string", "last_seen_at": "string",
            },
            src_key="id", src_param="source_service_id",
            tgt_key="id", tgt_param="target_service_id",
        ),
        "PRODUCES": _edge_def(
            "Service", "KafkaTopic",
            {
                "event_schema": "string",
                "ingestion_id": "string", "last_seen_at": "string",
            },
            src_key="id", src_param="service_id",
            tgt_key="name", tgt_param="topic_name",
        ),
        "CONSUMES": _edge_def(
            "Service", "KafkaTopic",
            {
                "consumer_group": "string",
                "ingestion_id": "string", "last_seen_at": "string",
            },
            src_key="id", src_param="service_id",
            tgt_key="name", tgt_param="topic_name",
        ),
        "DEPLOYED_IN": _edge_def(
            "Service", "K8sDeployment",
            {
                "ingestion_id": "string", "last_seen_at": "string",
            },
            src_key="id", src_param="service_id",
            tgt_key="id", tgt_param="deployment_id",
        ),
    }

    return Ontology(node_types=node_types, edge_types=edge_types)
