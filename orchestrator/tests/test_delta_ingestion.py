from __future__ import annotations

import pytest

from orchestrator.app.extraction_models import (
    DatabaseNode,
    KafkaTopicNode,
    K8sDeploymentNode,
    ServiceNode,
    compute_content_hash,
)
from orchestrator.app.neo4j_client import compute_hashes


class TestComputeContentHash:

    def test_produces_consistent_hash_for_same_input(self) -> None:
        entity = ServiceNode(
            id="svc-1",
            name="service-one",
            language="go",
            framework="gin",
            opentelemetry_enabled=True,
        )
        hash_a = compute_content_hash(entity)
        hash_b = compute_content_hash(entity)
        assert hash_a == hash_b
        assert len(hash_a) == 64
        assert all(c in "0123456789abcdef" for c in hash_a)

    def test_produces_different_hash_for_different_input(self) -> None:
        entity_a = ServiceNode(
            id="svc-1",
            name="service-one",
            language="go",
            framework="gin",
            opentelemetry_enabled=True,
        )
        entity_b = ServiceNode(
            id="svc-2",
            name="service-two",
            language="python",
            framework="fastapi",
            opentelemetry_enabled=False,
        )
        assert compute_content_hash(entity_a) != compute_content_hash(entity_b)

    def test_content_hash_field_defaults_to_empty_string(self) -> None:
        node = ServiceNode(
            id="x",
            name="x",
            language="py",
            framework="flask",
            opentelemetry_enabled=False,
        )
        assert node.content_hash == ""
        node = DatabaseNode(id="db-1", type="postgresql")
        assert node.content_hash == ""
        node = KafkaTopicNode(name="t", partitions=3, retention_ms=1000)
        assert node.content_hash == ""
        node = K8sDeploymentNode(id="d", namespace="ns", replicas=1)
        assert node.content_hash == ""

    def test_hash_excludes_content_hash_field_no_circular_reference(
        self,
    ) -> None:
        entity_a = ServiceNode(
            id="svc",
            name="svc",
            language="go",
            framework="gin",
            opentelemetry_enabled=True,
            content_hash="",
        )
        entity_b = ServiceNode(
            id="svc",
            name="svc",
            language="go",
            framework="gin",
            opentelemetry_enabled=True,
            content_hash="different-value",
        )
        assert compute_content_hash(entity_a) == compute_content_hash(entity_b)


class TestComputeHashes:

    def test_sets_content_hash_on_node_entities(self) -> None:
        entities = [
            ServiceNode(
                id="s1",
                name="s1",
                language="go",
                framework="gin",
                opentelemetry_enabled=True,
            ),
            DatabaseNode(id="d1", type="postgresql"),
        ]
        result = compute_hashes(entities)
        assert result[0].content_hash != ""
        assert result[1].content_hash != ""
        assert result[0].content_hash == compute_content_hash(entities[0])

    def test_leaves_entities_without_content_hash_unchanged(self) -> None:
        from orchestrator.app.extraction_models import CallsEdge

        entities = [
            CallsEdge(
                source_service_id="a",
                target_service_id="b",
                protocol="http",
            ),
        ]
        result = compute_hashes(entities)
        assert len(result) == 1
        assert result[0] is entities[0]
