from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from orchestrator.app.vector_reconciliation import VectorReconciliationJob


class _MetricsSpy:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, dict[str, str]]] = []

    def increment_counter(
        self,
        name: str,
        amount: int = 1,
        attributes: dict[str, str] | None = None,
    ) -> None:
        self.calls.append((name, amount, attributes or {}))

    def record_duration(self, name: str, duration_ms: float, attributes: dict[str, str]) -> None:
        _ = (name, duration_ms, attributes)


@pytest.mark.asyncio
async def test_reconciliation_upserts_missing_and_stale_vectors() -> None:
    graph_loader = AsyncMock(return_value={
        "a": {"vector": [0.1, 0.2], "metadata": {"name": "svc-a"}, "version": "2"},
        "b": {"vector": [0.3, 0.4], "metadata": {"name": "svc-b"}, "version": "1"},
        "c": {"vector": [0.5, 0.6], "metadata": {"name": "svc-c"}, "version": "1"},
    })
    vector_loader = AsyncMock(return_value={
        "a": {"vector": [0.1, 0.2], "metadata": {"name": "svc-a"}, "version": "1"},
        "b": {"vector": [0.3, 0.4], "metadata": {"name": "svc-b"}, "version": "1"},
    })
    vector_store = AsyncMock()
    metrics = _MetricsSpy()
    with pytest.MonkeyPatch.context() as m:
        m.setattr("orchestrator.app.vector_reconciliation.get_metrics_port", lambda: metrics)
        job = VectorReconciliationJob(graph_loader, vector_loader, vector_store)
        result = await job.run(collection="service_embeddings", tenant_id="t1")

    assert result.missing_count == 1
    assert result.stale_count == 1
    assert result.upserted_count == 2
    vector_store.upsert.assert_awaited_once()
    upsert_records = vector_store.upsert.await_args.args[1]
    assert {r.id for r in upsert_records} == {"a", "c"}
    assert ("vector_reconciliation.missing_count", 1, {"tenant_id": "t1", "collection": "service_embeddings"}) in metrics.calls
    assert ("vector_reconciliation.stale_count", 1, {"tenant_id": "t1", "collection": "service_embeddings"}) in metrics.calls


@pytest.mark.asyncio
async def test_reconciliation_noop_when_already_consistent() -> None:
    payload = {"x": {"vector": [0.1], "metadata": {}, "embedding_hash": "h1"}}
    graph_loader = AsyncMock(return_value=payload)
    vector_loader = AsyncMock(return_value=payload)
    vector_store = AsyncMock()
    metrics = _MetricsSpy()
    with pytest.MonkeyPatch.context() as m:
        m.setattr("orchestrator.app.vector_reconciliation.get_metrics_port", lambda: metrics)
        job = VectorReconciliationJob(graph_loader, vector_loader, vector_store)
        result = await job.run(collection="service_embeddings", tenant_id="t1")

    assert result == result.__class__(missing_count=0, stale_count=0, upserted_count=0)
    vector_store.upsert.assert_not_awaited()
    assert ("vector_reconciliation.upserted_count", 0, {"tenant_id": "t1", "collection": "service_embeddings"}) in metrics.calls
