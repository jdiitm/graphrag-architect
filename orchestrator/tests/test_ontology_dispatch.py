from __future__ import annotations

import pytest

from orchestrator.app.extraction_models import (
    CallsEdge,
    ConsumesEdge,
    DatabaseNode,
    DeployedInEdge,
    K8sDeploymentNode,
    KafkaTopicNode,
    ProducesEdge,
    ServiceNode,
)
from orchestrator.app.ontology import (
    EdgeTypeDefinition,
    NodeTypeDefinition,
    Ontology,
    generate_edge_merge_cypher,
    generate_edge_unwind_cypher,
    generate_merge_cypher,
    generate_unwind_cypher,
    build_default_ontology,
)


SAMPLE_SERVICE = ServiceNode(
    id="order-service",
    name="order-service",
    language="go",
    framework="gin",
    opentelemetry_enabled=True,
    tenant_id="test-tenant",
)

SAMPLE_DATABASE = DatabaseNode(
    id="orders-db", type="postgresql", tenant_id="test-tenant",
)

SAMPLE_TOPIC = KafkaTopicNode(
    name="order-events", partitions=6,
    retention_ms=604800000, tenant_id="test-tenant",
)

SAMPLE_K8S = K8sDeploymentNode(
    id="order-deploy", namespace="production",
    replicas=3, tenant_id="test-tenant",
)


class TestBuildDefaultOntology:

    def test_contains_all_four_node_types(self) -> None:
        ontology = build_default_ontology()
        labels = set(ontology.all_node_labels())
        assert labels == {"Service", "Database", "KafkaTopic", "K8sDeployment"}

    def test_contains_all_four_edge_types(self) -> None:
        ontology = build_default_ontology()
        edge_types = set(ontology.all_edge_types())
        assert edge_types == {"CALLS", "PRODUCES", "CONSUMES", "DEPLOYED_IN"}

    def test_service_node_properties_match_model(self) -> None:
        ontology = build_default_ontology()
        svc = ontology.get_node_type("Service")
        assert svc is not None
        assert svc.unique_key == "id"
        expected_props = {
            "id", "name", "language", "framework",
            "opentelemetry_enabled", "tenant_id",
            "team_owner", "namespace_acl", "confidence",
        }
        assert set(svc.properties.keys()) == expected_props

    def test_kafka_topic_unique_key_is_name(self) -> None:
        ontology = build_default_ontology()
        topic = ontology.get_node_type("KafkaTopic")
        assert topic is not None
        assert topic.unique_key == "name"

    def test_calls_edge_endpoints(self) -> None:
        ontology = build_default_ontology()
        calls = ontology.get_edge_type("CALLS")
        assert calls is not None
        assert calls.source_type == "Service"
        assert calls.target_type == "Service"

    def test_produces_edge_endpoints(self) -> None:
        ontology = build_default_ontology()
        prod = ontology.get_edge_type("PRODUCES")
        assert prod is not None
        assert prod.source_type == "Service"
        assert prod.target_type == "KafkaTopic"

    def test_deployed_in_edge_endpoints(self) -> None:
        ontology = build_default_ontology()
        dep = ontology.get_edge_type("DEPLOYED_IN")
        assert dep is not None
        assert dep.source_type == "Service"
        assert dep.target_type == "K8sDeployment"


class TestEdgeCypherGeneration:

    def test_edge_merge_cypher_calls(self) -> None:
        edge_def = EdgeTypeDefinition(
            source_type="Service",
            target_type="Service",
            properties={
                "protocol": "string", "confidence": "float",
                "ingestion_id": "string", "last_seen_at": "string",
            },
            source_key="id",
            target_key="id",
            source_label="Service",
            target_label="Service",
        )
        cypher = generate_edge_merge_cypher("CALLS", edge_def)
        assert "MATCH" in cypher
        assert "MERGE" in cypher
        assert ":CALLS" in cypher
        assert "$protocol" in cypher

    def test_edge_unwind_cypher_calls(self) -> None:
        edge_def = EdgeTypeDefinition(
            source_type="Service",
            target_type="Service",
            properties={
                "protocol": "string", "confidence": "float",
                "ingestion_id": "string", "last_seen_at": "string",
            },
            source_key="id",
            target_key="id",
            source_label="Service",
            target_label="Service",
        )
        cypher = generate_edge_unwind_cypher("CALLS", edge_def)
        assert "UNWIND $batch" in cypher
        assert "MERGE" in cypher
        assert ":CALLS" in cypher
        assert "row.protocol" in cypher

    def test_edge_merge_cypher_produces(self) -> None:
        edge_def = EdgeTypeDefinition(
            source_type="Service",
            target_type="KafkaTopic",
            properties={
                "event_schema": "string",
                "ingestion_id": "string",
                "last_seen_at": "string",
            },
            source_key="id",
            target_key="name",
            source_label="Service",
            target_label="KafkaTopic",
        )
        cypher = generate_edge_merge_cypher("PRODUCES", edge_def)
        assert ":Service" in cypher
        assert ":KafkaTopic" in cypher
        assert ":PRODUCES" in cypher


class TestOntologyAllEdgeTypes:

    def test_all_edge_types_returns_keys(self) -> None:
        ontology = Ontology(
            node_types={},
            edge_types={
                "CALLS": EdgeTypeDefinition(
                    source_type="Service", target_type="Service",
                ),
                "USES": EdgeTypeDefinition(
                    source_type="Service", target_type="Database",
                ),
            },
        )
        result = ontology.all_edge_types()
        assert set(result) == {"CALLS", "USES"}


class TestOntologyDrivenNodeCypherParity:

    def test_service_merge_matches_hardcoded(self) -> None:
        ontology = build_default_ontology()
        svc_def = ontology.get_node_type("Service")
        cypher = generate_merge_cypher("Service", svc_def)
        assert "MERGE (n:Service {id: $id})" in cypher
        assert "n.tenant_id = $tenant_id" in cypher
        assert "n.name = $name" in cypher
        assert "n.language = $language" in cypher

    def test_service_unwind_matches_hardcoded(self) -> None:
        ontology = build_default_ontology()
        svc_def = ontology.get_node_type("Service")
        cypher = generate_unwind_cypher("Service", svc_def)
        assert "UNWIND $batch AS row" in cypher
        assert "MERGE (n:Service {id: row.id})" in cypher
        assert "n.tenant_id = row.tenant_id" in cypher

    def test_kafka_topic_merge_uses_name_key(self) -> None:
        ontology = build_default_ontology()
        topic_def = ontology.get_node_type("KafkaTopic")
        cypher = generate_merge_cypher("KafkaTopic", topic_def)
        assert "MERGE (n:KafkaTopic {name: $name})" in cypher
        assert "n.tenant_id = $tenant_id" in cypher
