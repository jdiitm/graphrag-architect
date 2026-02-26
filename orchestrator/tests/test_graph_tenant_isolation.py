from __future__ import annotations

import os
import re
from typing import Any, Dict, List

import pytest
from pydantic import ValidationError

from orchestrator.app.extraction_models import (
    DatabaseNode,
    K8sDeploymentNode,
    KafkaTopicNode,
    ServiceNode,
)


_SCHEMA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "app", "schema_init.cypher",
)

_K8S_SCHEMA_JOB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "infrastructure", "k8s", "neo4j-schema-job.yaml",
)

_NODE_LABELS = ("Service", "Database", "KafkaTopic", "K8sDeployment")


def _read_file(path: str) -> str:
    with open(path, encoding="utf-8") as fh:
        return fh.read()


class TestSchemaNotNullConstraints:

    def test_tenant_id_node_key_constraint_exists_for_all_labels(self) -> None:
        schema = _read_file(_SCHEMA_PATH)
        for label in _NODE_LABELS:
            pattern = (
                rf"CREATE\s+CONSTRAINT\s+\S+\s+IF\s+NOT\s+EXISTS\s+"
                rf"FOR\s+\(\w+:{re.escape(label)}\)\s+"
                rf"REQUIRE\s+\(.*?tenant_id.*?\)\s+IS\s+NODE\s+KEY"
            )
            assert re.search(pattern, schema, re.IGNORECASE), (
                f"Missing composite NODE KEY constraint with tenant_id on "
                f"{label} in {_SCHEMA_PATH}"
            )

    def test_tenant_id_index_exists_for_all_labels(self) -> None:
        schema = _read_file(_SCHEMA_PATH)
        for label in _NODE_LABELS:
            pattern = (
                rf"CREATE\s+INDEX\s+\S+\s+IF\s+NOT\s+EXISTS\s+"
                rf"FOR\s+\(\w+:{re.escape(label)}\)\s+"
                rf"ON\s+\(\w+\.tenant_id\)"
            )
            assert re.search(pattern, schema, re.IGNORECASE), (
                f"Missing index on tenant_id for {label} in {_SCHEMA_PATH}"
            )


class TestK8sConfigMapSync:

    def test_configmap_contains_composite_node_key_constraints(self) -> None:
        k8s_yaml = _read_file(_K8S_SCHEMA_JOB_PATH)
        assert "IS NODE KEY" in k8s_yaml, (
            "K8s ConfigMap missing composite NODE KEY constraints"
        )

    def test_configmap_constraint_count_matches_canonical(self) -> None:
        schema = _read_file(_SCHEMA_PATH)
        k8s_yaml = _read_file(_K8S_SCHEMA_JOB_PATH)
        canonical_count = schema.lower().count("is node key")
        k8s_count = k8s_yaml.lower().count("is node key")
        assert k8s_count == canonical_count, (
            f"Canonical schema has {canonical_count} NODE KEY "
            f"constraints but K8s ConfigMap has {k8s_count}"
        )


class TestPydanticTenantIdEnforcement:

    def test_service_node_rejects_empty_tenant_id(self) -> None:
        with pytest.raises(ValidationError, match="tenant_id"):
            ServiceNode(
                id="svc", name="svc", language="go",
                framework="gin", opentelemetry_enabled=True, tenant_id="",
            )

    def test_service_node_rejects_missing_tenant_id(self) -> None:
        with pytest.raises(ValidationError, match="tenant_id"):
            ServiceNode(
                id="svc", name="svc", language="go",
                framework="gin", opentelemetry_enabled=True,
            )  # type: ignore[call-arg]

    def test_database_node_rejects_empty_tenant_id(self) -> None:
        with pytest.raises(ValidationError, match="tenant_id"):
            DatabaseNode(id="db", type="postgres", tenant_id="")

    def test_kafka_topic_rejects_empty_tenant_id(self) -> None:
        with pytest.raises(ValidationError, match="tenant_id"):
            KafkaTopicNode(
                name="t", partitions=1, retention_ms=100, tenant_id="",
            )

    def test_k8s_deployment_rejects_empty_tenant_id(self) -> None:
        with pytest.raises(ValidationError, match="tenant_id"):
            K8sDeploymentNode(
                id="d", namespace="ns", replicas=1, tenant_id="",
            )


class TestCypherQueriesIncludeTenantId:

    def test_all_merge_queries_set_tenant_id(self) -> None:
        from orchestrator.app.neo4j_client import _UNWIND_QUERIES

        node_types = (ServiceNode, DatabaseNode, KafkaTopicNode, K8sDeploymentNode)
        for node_type in node_types:
            query = _UNWIND_QUERIES.get(node_type, "")
            assert "tenant_id" in query, (
                f"UNWIND query for {node_type.__name__} does not SET tenant_id"
            )

    def test_single_entity_queries_set_tenant_id(self) -> None:
        from orchestrator.app.neo4j_client import (
            _service_cypher,
            _database_cypher,
            _kafka_topic_cypher,
            _k8s_deployment_cypher,
        )

        svc = ServiceNode(
            id="s", name="s", language="go", framework="gin",
            opentelemetry_enabled=True, tenant_id="t1",
        )
        db = DatabaseNode(id="d", type="pg", tenant_id="t1")
        topic = KafkaTopicNode(
            name="t", partitions=1, retention_ms=1000, tenant_id="t1",
        )
        deploy = K8sDeploymentNode(
            id="k", namespace="ns", replicas=1, tenant_id="t1",
        )

        for entity, gen in [
            (svc, _service_cypher),
            (db, _database_cypher),
            (topic, _kafka_topic_cypher),
            (deploy, _k8s_deployment_cypher),
        ]:
            cypher, params = gen(entity)
            assert "tenant_id" in cypher, (
                f"Single-entity Cypher for {type(entity).__name__} "
                f"missing tenant_id"
            )
            assert params.get("tenant_id") == "t1", (
                f"Params for {type(entity).__name__} missing tenant_id=t1"
            )
