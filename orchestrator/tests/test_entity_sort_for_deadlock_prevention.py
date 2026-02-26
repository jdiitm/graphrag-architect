from __future__ import annotations

from typing import Any, List

import pytest

from orchestrator.app.extraction_models import (
    CallsEdge,
    DatabaseNode,
    K8sDeploymentNode,
    KafkaTopicNode,
    ServiceNode,
)
from orchestrator.app.neo4j_client import _sort_entities_for_write


class TestEntitySortForDeadlockPrevention:

    def test_sorts_by_type_name_then_id(self) -> None:
        entities: List[Any] = [
            ServiceNode(
                id="svc-z", name="zulu", language="go",
                framework="gin", opentelemetry_enabled=False, tenant_id="t1",
            ),
            DatabaseNode(id="db-a", type="postgresql", tenant_id="t1"),
            ServiceNode(
                id="svc-a", name="alpha", language="python",
                framework="fastapi", opentelemetry_enabled=True, tenant_id="t1",
            ),
        ]
        sorted_entities = _sort_entities_for_write(entities)
        type_id_pairs = [
            (type(e).__name__, getattr(e, "id", "") or getattr(e, "name", ""))
            for e in sorted_entities
        ]
        assert type_id_pairs == [
            ("DatabaseNode", "db-a"),
            ("ServiceNode", "svc-a"),
            ("ServiceNode", "svc-z"),
        ]

    def test_sorts_edges_deterministically(self) -> None:
        entities: List[Any] = [
            CallsEdge(
                source_service_id="svc-b", target_service_id="svc-c",
                protocol="grpc", tenant_id="test-tenant",
            ),
            CallsEdge(
                source_service_id="svc-a", target_service_id="svc-b",
                protocol="http", tenant_id="test-tenant",
            ),
        ]
        sorted_entities = _sort_entities_for_write(entities)
        ids = [e.source_service_id for e in sorted_entities]
        assert ids == ["svc-a", "svc-b"]

    def test_mixed_types_sorted_by_class_name(self) -> None:
        entities: List[Any] = [
            KafkaTopicNode(
                name="z-topic", partitions=3,
                retention_ms=604800000, tenant_id="t1",
            ),
            K8sDeploymentNode(
                id="k8s-1", namespace="prod", replicas=2, tenant_id="t1",
            ),
            DatabaseNode(id="db-1", type="redis", tenant_id="t1"),
        ]
        sorted_entities = _sort_entities_for_write(entities)
        type_names = [type(e).__name__ for e in sorted_entities]
        assert type_names == ["DatabaseNode", "K8sDeploymentNode", "KafkaTopicNode"]

    def test_empty_list_returns_empty(self) -> None:
        assert _sort_entities_for_write([]) == []

    def test_single_entity_unchanged(self) -> None:
        entity = ServiceNode(
            id="svc-only", name="only", language="go",
            framework="gin", opentelemetry_enabled=False, tenant_id="t1",
        )
        result = _sort_entities_for_write([entity])
        assert len(result) == 1
        assert result[0].id == "svc-only"

    def test_sort_is_stable_for_identical_keys(self) -> None:
        a = ServiceNode(
            id="svc-x", name="svc-x", language="go",
            framework="gin", opentelemetry_enabled=False, tenant_id="t1",
        )
        b = ServiceNode(
            id="svc-x", name="svc-x", language="python",
            framework="fastapi", opentelemetry_enabled=True, tenant_id="t2",
        )
        result = _sort_entities_for_write([a, b])
        assert result[0].language == "go"
        assert result[1].language == "python"
