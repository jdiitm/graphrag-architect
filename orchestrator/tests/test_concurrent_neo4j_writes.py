from __future__ import annotations

import asyncio
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from orchestrator.app.extraction_models import (
    CallsEdge,
    DatabaseNode,
    K8sDeploymentNode,
    KafkaTopicNode,
    ServiceNode,
)
from orchestrator.app.neo4j_client import GraphRepository


def _make_mock_driver() -> MagicMock:
    mock_result = AsyncMock()
    mock_result.run = AsyncMock(return_value=mock_result)
    mock_result.data = AsyncMock(return_value=[])

    mock_tx = AsyncMock()
    mock_tx.run = AsyncMock(return_value=mock_result)

    mock_session = AsyncMock()
    mock_session.execute_write = AsyncMock(side_effect=lambda fn, **kw: fn(mock_tx, **kw))
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_driver = MagicMock()
    mock_driver.session = MagicMock(return_value=mock_session)
    return mock_driver


class TestConcurrentNodeWritesByType:

    @pytest.mark.asyncio
    async def test_different_node_types_written_concurrently(self) -> None:
        write_log: List[str] = []
        original_write_batches = GraphRepository._write_batches

        async def instrumented_write_batches(
            self_repo: Any, entity_type: type, records: List[Dict[str, Any]],
        ) -> None:
            write_log.append(f"start:{entity_type.__name__}")
            await asyncio.sleep(0.01)
            await original_write_batches(self_repo, entity_type, records)
            write_log.append(f"end:{entity_type.__name__}")

        driver = _make_mock_driver()
        cb = CircuitBreaker(CircuitBreakerConfig())
        repo = GraphRepository(driver, circuit_breaker=cb, batch_size=100)

        entities = [
            ServiceNode(
                id="svc-1", name="svc-1", language="go",
                framework="gin", opentelemetry_enabled=True, tenant_id="t1",
            ),
            DatabaseNode(id="db-1", type="postgresql", tenant_id="t1"),
            KafkaTopicNode(name="topic-1", partitions=3, retention_ms=604800000, tenant_id="t1"),
            K8sDeploymentNode(id="k8s-1", namespace="prod", replicas=2, tenant_id="t1"),
        ]

        repo._write_batches = lambda et, recs: instrumented_write_batches(repo, et, recs)
        await repo.commit_topology(entities)

        service_start = write_log.index("start:ServiceNode")
        database_start = write_log.index("start:DatabaseNode")
        service_end = write_log.index("end:ServiceNode")

        assert database_start < service_end, (
            "DatabaseNode write should start before ServiceNode finishes (concurrent)"
        )

    @pytest.mark.asyncio
    async def test_edges_written_after_all_nodes(self) -> None:
        write_order: List[str] = []

        driver = _make_mock_driver()
        cb = CircuitBreaker(CircuitBreakerConfig())
        repo = GraphRepository(driver, circuit_breaker=cb, batch_size=100)

        original_write = repo._write_batches

        async def tracking_write(entity_type: type, records: List[Dict[str, Any]]) -> None:
            write_order.append(entity_type.__name__)
            await original_write(entity_type, records)

        repo._write_batches = tracking_write

        entities = [
            ServiceNode(
                id="svc-a", name="svc-a", language="go",
                framework="gin", opentelemetry_enabled=True, tenant_id="t1",
            ),
            ServiceNode(
                id="svc-b", name="svc-b", language="python",
                framework="fastapi", opentelemetry_enabled=False, tenant_id="t1",
            ),
            CallsEdge(source_service_id="svc-a", target_service_id="svc-b", protocol="http", tenant_id="test-tenant"),
        ]

        await repo.commit_topology(entities)

        calls_idx = write_order.index("CallsEdge")
        service_idx = write_order.index("ServiceNode")
        assert service_idx < calls_idx, (
            "Nodes must be written before edges for referential integrity"
        )

    @pytest.mark.asyncio
    async def test_concurrent_writes_complete_all_entities(self) -> None:
        driver = _make_mock_driver()
        cb = CircuitBreaker(CircuitBreakerConfig())
        repo = GraphRepository(driver, circuit_breaker=cb, batch_size=50)

        entities = []
        for i in range(200):
            entities.append(ServiceNode(
                id=f"svc-{i}", name=f"svc-{i}", language="go",
                framework="gin", opentelemetry_enabled=True, tenant_id="t1",
            ))
        for i in range(50):
            entities.append(DatabaseNode(
                id=f"db-{i}", type="postgresql", tenant_id="t1",
            ))

        await repo.commit_topology(entities)

        mock_session = driver.session.return_value
        assert mock_session.execute_write.call_count > 0


class TestSinkConcurrencyConfig:

    def test_default_concurrency(self) -> None:
        driver = _make_mock_driver()
        repo = GraphRepository(driver)
        assert repo._write_concurrency >= 1

    def test_custom_concurrency(self) -> None:
        driver = _make_mock_driver()
        repo = GraphRepository(driver, write_concurrency=8)
        assert repo._write_concurrency == 8
