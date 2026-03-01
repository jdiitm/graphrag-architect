from __future__ import annotations

import random

import pytest

from orchestrator.app.extraction_models import (
    CallsEdge,
    DatabaseNode,
    ServiceNode,
)
from orchestrator.app.neo4j_client import (
    _entity_sort_key,
    _sort_entities_for_write,
    detect_hot_targets,
)


class TestDeterministicWriteOrdering:

    def test_sort_is_stable_across_shuffles(self) -> None:
        entities = [
            ServiceNode(
                id="svc-c", name="svc-c", language="go",
                framework="gin", opentelemetry_enabled=True, tenant_id="t1",
            ),
            ServiceNode(
                id="svc-a", name="svc-a", language="python",
                framework="fastapi", opentelemetry_enabled=True, tenant_id="t1",
            ),
            DatabaseNode(id="db-b", type="postgres", tenant_id="t1"),
            DatabaseNode(id="db-a", type="redis", tenant_id="t1"),
        ]
        canonical_order = _sort_entities_for_write(list(entities))
        canonical_keys = [_entity_sort_key(e) for e in canonical_order]

        for _ in range(10):
            shuffled = list(entities)
            random.shuffle(shuffled)
            result = _sort_entities_for_write(shuffled)
            result_keys = [_entity_sort_key(e) for e in result]
            assert result_keys == canonical_keys, (
                "Sort must produce identical ordering regardless of input order"
            )

    def test_edges_sorted_within_type(self) -> None:
        edges = [
            CallsEdge(
                source_service_id="svc-z", target_service_id="svc-a",
                protocol="grpc", tenant_id="t1",
            ),
            CallsEdge(
                source_service_id="svc-a", target_service_id="svc-b",
                protocol="http", tenant_id="t1",
            ),
        ]
        result = _sort_entities_for_write(edges)
        keys = [_entity_sort_key(e) for e in result]
        assert keys == sorted(keys)

    def test_mixed_types_sorted_deterministically(self) -> None:
        entities = [
            ServiceNode(
                id="svc-a", name="svc-a", language="go",
                framework="gin", opentelemetry_enabled=True, tenant_id="t1",
            ),
            CallsEdge(
                source_service_id="svc-a", target_service_id="svc-b",
                protocol="http", tenant_id="t1",
            ),
        ]
        result = _sort_entities_for_write(entities)
        keys = [_entity_sort_key(e) for e in result]
        assert keys[0][0] == "CallsEdge", (
            "Sort by (type_name, id) puts CallsEdge before ServiceNode "
            "alphabetically -- deterministic ordering prevents deadlocks"
        )
        assert keys == sorted(keys)


class TestHotTargetDetection:

    def test_function_exists(self) -> None:
        assert callable(detect_hot_targets)

    def test_detects_high_degree_target(self) -> None:
        edges = [
            CallsEdge(
                source_service_id=f"svc-{i}",
                target_service_id="shared-db",
                protocol="http",
                tenant_id="t1",
            )
            for i in range(20)
        ]
        hot = detect_hot_targets(edges, threshold=10)
        assert "shared-db" in hot, (
            "A target node receiving 20 edges must be detected as hot "
            "when threshold is 10"
        )

    def test_no_hot_targets_below_threshold(self) -> None:
        edges = [
            CallsEdge(
                source_service_id=f"svc-{i}",
                target_service_id=f"target-{i}",
                protocol="http",
                tenant_id="t1",
            )
            for i in range(5)
        ]
        hot = detect_hot_targets(edges, threshold=10)
        assert len(hot) == 0

    def test_multiple_hot_targets(self) -> None:
        edges = []
        for i in range(15):
            edges.append(CallsEdge(
                source_service_id=f"svc-{i}",
                target_service_id="hot-a",
                protocol="http",
                tenant_id="t1",
            ))
        for i in range(12):
            edges.append(CallsEdge(
                source_service_id=f"svc-x{i}",
                target_service_id="hot-b",
                protocol="grpc",
                tenant_id="t1",
            ))
        hot = detect_hot_targets(edges, threshold=10)
        assert "hot-a" in hot
        assert "hot-b" in hot
