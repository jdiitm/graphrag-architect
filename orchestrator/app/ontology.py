from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


@dataclass(frozen=True)
class NodeTypeDefinition:
    properties: Dict[str, str]
    unique_key: str
    acl_fields: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class EdgeTypeDefinition:
    source_type: str
    target_type: str
    properties: Dict[str, str] = field(default_factory=dict)


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
                acl_fields=definition.get("acl_fields", []),
            )

        edge_types: Dict[str, EdgeTypeDefinition] = {}
        for rel_type, definition in data.get("edge_types", {}).items():
            edge_types[rel_type] = EdgeTypeDefinition(
                source_type=definition.get("source_type", ""),
                target_type=definition.get("target_type", ""),
                properties=definition.get("properties", {}),
            )

        return Ontology(node_types=node_types, edge_types=edge_types)


def generate_merge_cypher(
    label: str, node_def: NodeTypeDefinition,
) -> str:
    key = node_def.unique_key
    set_parts = [
        f"n.{prop} = ${prop}"
        for prop in node_def.properties
        if prop != key
    ]
    set_clause = ", ".join(set_parts)
    return (
        f"MERGE (n:{label} {{{key}: ${key}}}) "
        f"SET {set_clause}"
    )


def generate_unwind_cypher(
    label: str, node_def: NodeTypeDefinition,
) -> str:
    key = node_def.unique_key
    set_parts = [
        f"n.{prop} = row.{prop}"
        for prop in node_def.properties
        if prop != key
    ]
    set_clause = ", ".join(set_parts)
    return (
        f"UNWIND $batch AS row "
        f"MERGE (n:{label} {{{key}: row.{key}}}) "
        f"SET {set_clause}"
    )
