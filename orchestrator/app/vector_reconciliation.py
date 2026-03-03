from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

from orchestrator.app.telemetry_ports import get_metrics_port
from orchestrator.app.vector_store import VectorRecord

Loader = Callable[[str, str], Awaitable[Dict[str, Dict[str, Any]]]]


@dataclass(frozen=True)
class VectorReconciliationResult:
    missing_count: int
    stale_count: int
    upserted_count: int


class VectorReconciliationJob:
    def __init__(
        self,
        graph_loader: Loader,
        vector_loader: Loader,
        vector_store: Any,
    ) -> None:
        self._graph_loader = graph_loader
        self._vector_loader = vector_loader
        self._vector_store = vector_store

    async def run(
        self,
        collection: str,
        tenant_id: str,
    ) -> VectorReconciliationResult:
        graph_vectors = await self._graph_loader(collection, tenant_id)
        vector_index = await self._vector_loader(collection, tenant_id)

        missing_ids = sorted(set(graph_vectors) - set(vector_index))
        stale_ids = sorted(
            node_id for node_id in set(graph_vectors) & set(vector_index)
            if _version_token(graph_vectors[node_id]) != _version_token(vector_index[node_id])
        )
        reconcile_ids = missing_ids + stale_ids
        records = [_to_vector_record(node_id, graph_vectors[node_id]) for node_id in reconcile_ids]
        if records:
            await self._vector_store.upsert(collection, records, tenant_id=tenant_id)

        metrics = get_metrics_port()
        tags = {"tenant_id": tenant_id, "collection": collection}
        metrics.increment_counter("vector_reconciliation.missing_count", len(missing_ids), tags)
        metrics.increment_counter("vector_reconciliation.stale_count", len(stale_ids), tags)
        metrics.increment_counter("vector_reconciliation.upserted_count", len(records), tags)
        return VectorReconciliationResult(
            missing_count=len(missing_ids),
            stale_count=len(stale_ids),
            upserted_count=len(records),
        )


def _version_token(payload: Dict[str, Any]) -> Optional[str]:
    for key in ("embedding_hash", "version", "updated_at"):
        value = payload.get(key)
        if value is not None:
            return str(value)
    return None


def _to_vector_record(node_id: str, payload: Dict[str, Any]) -> VectorRecord:
    return VectorRecord(
        id=node_id,
        vector=list(payload.get("vector", [])),
        metadata=dict(payload.get("metadata", {})),
    )
