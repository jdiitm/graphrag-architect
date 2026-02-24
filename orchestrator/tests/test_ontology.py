from __future__ import annotations

import tempfile
import os

import pytest

from orchestrator.app.ontology import (
    EdgeTypeDefinition,
    NodeTypeDefinition,
    Ontology,
    OntologyLoader,
    generate_merge_cypher,
    generate_unwind_cypher,
)


SAMPLE_YAML = """
node_types:
  Service:
    properties:
      id: string
      name: string
      language: string
      framework: string
    unique_key: id
    acl_fields:
      - team_owner
      - namespace_acl
  CustomType:
    properties:
      id: string
      category: string
    unique_key: id
    acl_fields: []

edge_types:
  CALLS:
    source_type: Service
    target_type: Service
    properties:
      protocol: string
  USES:
    source_type: Service
    target_type: CustomType
    properties:
      frequency: string
"""


class TestOntologyLoader:
    def test_load_from_yaml_string(self) -> None:
        ontology = OntologyLoader.from_yaml_string(SAMPLE_YAML)
        assert len(ontology.node_types) == 2
        assert "Service" in ontology.node_types
        assert "CustomType" in ontology.node_types

    def test_load_from_file(self) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False,
        ) as fh:
            fh.write(SAMPLE_YAML)
            fh.flush()
            path = fh.name
        try:
            ontology = OntologyLoader.from_file(path)
            assert len(ontology.node_types) == 2
        finally:
            os.unlink(path)

    def test_node_type_properties(self) -> None:
        ontology = OntologyLoader.from_yaml_string(SAMPLE_YAML)
        svc = ontology.node_types["Service"]
        assert svc.unique_key == "id"
        assert "name" in svc.properties
        assert "language" in svc.properties

    def test_edge_type_properties(self) -> None:
        ontology = OntologyLoader.from_yaml_string(SAMPLE_YAML)
        assert len(ontology.edge_types) == 2
        calls = ontology.edge_types["CALLS"]
        assert calls.source_type == "Service"
        assert calls.target_type == "Service"

    def test_acl_fields(self) -> None:
        ontology = OntologyLoader.from_yaml_string(SAMPLE_YAML)
        svc = ontology.node_types["Service"]
        assert "team_owner" in svc.acl_fields
        custom = ontology.node_types["CustomType"]
        assert len(custom.acl_fields) == 0


class TestCypherGeneration:
    def test_merge_cypher_for_known_type(self) -> None:
        ontology = OntologyLoader.from_yaml_string(SAMPLE_YAML)
        node_def = ontology.node_types["Service"]
        cypher = generate_merge_cypher("Service", node_def)
        assert "MERGE" in cypher
        assert "Service" in cypher
        assert "id:" in cypher or "$id" in cypher

    def test_merge_cypher_for_custom_type(self) -> None:
        ontology = OntologyLoader.from_yaml_string(SAMPLE_YAML)
        node_def = ontology.node_types["CustomType"]
        cypher = generate_merge_cypher("CustomType", node_def)
        assert "MERGE" in cypher
        assert "CustomType" in cypher

    def test_unwind_cypher(self) -> None:
        ontology = OntologyLoader.from_yaml_string(SAMPLE_YAML)
        node_def = ontology.node_types["Service"]
        cypher = generate_unwind_cypher("Service", node_def)
        assert "UNWIND" in cypher
        assert "$batch" in cypher
        assert "Service" in cypher

    def test_unwind_sets_all_properties(self) -> None:
        ontology = OntologyLoader.from_yaml_string(SAMPLE_YAML)
        node_def = ontology.node_types["Service"]
        cypher = generate_unwind_cypher("Service", node_def)
        for prop in ("name", "language", "framework"):
            assert prop in cypher


class TestOntology:
    def test_get_node_type(self) -> None:
        ontology = OntologyLoader.from_yaml_string(SAMPLE_YAML)
        assert ontology.get_node_type("Service") is not None
        assert ontology.get_node_type("Unknown") is None

    def test_get_edge_type(self) -> None:
        ontology = OntologyLoader.from_yaml_string(SAMPLE_YAML)
        assert ontology.get_edge_type("CALLS") is not None
        assert ontology.get_edge_type("UNKNOWN") is None

    def test_all_labels(self) -> None:
        ontology = OntologyLoader.from_yaml_string(SAMPLE_YAML)
        labels = ontology.all_node_labels()
        assert "Service" in labels
        assert "CustomType" in labels
