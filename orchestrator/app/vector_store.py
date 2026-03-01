from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VectorRecord:
    id: str
    vector: List[float]
    metadata: Dict[str, Any]


@dataclass(frozen=True)
class SearchResult:
    id: str
    score: float
    metadata: Dict[str, Any]


def resolve_collection_name(
    base_collection: str, tenant_id: Optional[str],
) -> str:
    if not tenant_id:
        return base_collection
    safe: list[str] = []
    for ch in tenant_id:
        if ch.isalnum() or ch == "-":
            safe.append(ch)
        else:
            safe.append(f"_{ord(ch):02x}")
    return f"{base_collection}__{''.join(safe)}"


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    if len(a) != len(b):
        raise ValueError("Vectors must have the same dimension")
    if not a:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


@runtime_checkable
class VectorStore(Protocol):
    async def upsert(
        self, collection: str, vectors: List[VectorRecord]
    ) -> int:
        ...

    async def search(
        self,
        collection: str,
        query_vector: List[float],
        limit: int = 10,
    ) -> List[SearchResult]:
        ...

    async def delete(self, collection: str, ids: List[str], tenant_id: str = "") -> int:
        ...


class InMemoryVectorStore:
    def __init__(self) -> None:
        self._store: Dict[str, Dict[str, VectorRecord]] = {}

    async def upsert(
        self, collection: str, vectors: List[VectorRecord]
    ) -> int:
        if collection not in self._store:
            self._store[collection] = {}
        for record in vectors:
            self._store[collection][record.id] = record
        return len(vectors)

    async def search(
        self,
        collection: str,
        query_vector: List[float],
        limit: int = 10,
    ) -> List[SearchResult]:
        if collection not in self._store:
            return []
        records = list(self._store[collection].values())
        scored: List[tuple[VectorRecord, float]] = []
        for record in records:
            score = _cosine_similarity(query_vector, record.vector)
            scored.append((record, score))
        scored.sort(key=lambda pair: pair[1], reverse=True)
        return [
            SearchResult(id=r.id, score=s, metadata=r.metadata)
            for r, s in scored[:limit]
        ]

    async def delete(self, collection: str, ids: List[str], tenant_id: str = "") -> int:
        if collection not in self._store:
            return 0
        removed = 0
        for record_id in ids:
            record = self._store[collection].get(record_id)
            if record is None:
                continue
            if tenant_id and record.metadata.get("tenant_id") != tenant_id:
                continue
            del self._store[collection][record_id]
            removed += 1
        return removed

    async def search_with_tenant(
        self,
        collection: str,
        query_vector: List[float],
        tenant_id: str,
        limit: int = 10,
    ) -> List[SearchResult]:
        if collection not in self._store:
            return []
        records = list(self._store[collection].values())
        scored: List[tuple[VectorRecord, float]] = []
        for record in records:
            if tenant_id and record.metadata.get("tenant_id") != tenant_id:
                continue
            score = _cosine_similarity(query_vector, record.vector)
            scored.append((record, score))
        scored.sort(key=lambda pair: pair[1], reverse=True)
        return [
            SearchResult(id=r.id, score=s, metadata=r.metadata)
            for r, s in scored[:limit]
        ]


class QdrantVectorStore:
    is_read_replica: bool = True

    def __init__(
        self,
        url: str = "http://localhost:6333",
        api_key: str = "",
        prefer_grpc: bool = True,
    ) -> None:
        self._url = url
        self._api_key = api_key
        self._prefer_grpc = prefer_grpc
        self._client: Optional[Any] = None

    def _get_client(self) -> Any:
        if self._client is None:
            try:
                from qdrant_client import AsyncQdrantClient as _AQC
                self._client = _AQC(
                    url=self._url,
                    api_key=self._api_key or None,
                    prefer_grpc=self._prefer_grpc,
                )
            except ImportError as exc:
                raise RuntimeError(
                    "qdrant-client is required for QdrantVectorStore. "
                    "Install with: pip install qdrant-client"
                ) from exc
        return self._client

    async def upsert(
        self, collection: str, vectors: List[VectorRecord],
    ) -> int:
        client = self._get_client()
        from qdrant_client.models import PointStruct
        points = [
            PointStruct(
                id=record.id,
                vector=record.vector,
                payload=record.metadata,
            )
            for record in vectors
        ]
        await client.upsert(collection_name=collection, points=points)
        return len(vectors)

    async def search(
        self,
        collection: str,
        query_vector: List[float],
        limit: int = 10,
    ) -> List[SearchResult]:
        client = self._get_client()
        results = await client.search(
            collection_name=collection,
            query_vector=query_vector,
            limit=limit,
        )
        return [
            SearchResult(
                id=str(hit.id),
                score=hit.score,
                metadata=hit.payload or {},
            )
            for hit in results
        ]

    async def delete(self, collection: str, ids: List[str], tenant_id: str = "") -> int:
        client = self._get_client()
        from qdrant_client.models import PointIdsList
        if tenant_id:
            from qdrant_client.models import (
                FieldCondition, Filter, HasIdCondition, MatchValue,
            )
            compound = Filter(
                must=[
                    HasIdCondition(has_id=ids),
                    FieldCondition(
                        key="tenant_id", match=MatchValue(value=tenant_id),
                    ),
                ],
            )
            count_result = await client.count(
                collection_name=collection, count_filter=compound, exact=True,
            )
            await client.delete(
                collection_name=collection, points_selector=compound,
            )
            return count_result.count
        await client.delete(
            collection_name=collection,
            points_selector=PointIdsList(points=ids),
        )
        return len(ids)

    async def search_with_tenant(
        self,
        collection: str,
        query_vector: List[float],
        tenant_id: str,
        limit: int = 10,
    ) -> List[SearchResult]:
        client = self._get_client()
        from qdrant_client.models import FieldCondition, Filter, MatchValue
        query_filter = Filter(
            must=[
                FieldCondition(
                    key="tenant_id",
                    match=MatchValue(value=tenant_id),
                ),
            ],
        )
        results = await client.search(
            collection_name=collection,
            query_vector=query_vector,
            query_filter=query_filter,
            limit=limit,
        )
        return [
            SearchResult(
                id=str(hit.id),
                score=hit.score,
                metadata=hit.payload or {},
            )
            for hit in results
        ]


def create_vector_store(
    backend: str = "memory",
    url: str = "",
    api_key: str = "",
    driver: Any = None,
    pool_size: int = 4,
    deployment_mode: str = "dev",
) -> Any:
    _ = pool_size
    _ = driver
    if backend == "neo4j":
        raise ValueError(
            "Neo4j vector backend has been removed. Storing embeddings as "
            "Neo4j node properties causes PageCache pollution and recall "
            "collapse under multi-tenant post-filtering. "
            "Use VECTOR_STORE_BACKEND=qdrant instead."
        )
    if deployment_mode == "production" and backend != "qdrant":
        raise ValueError(
            f"VECTOR_STORE_BACKEND={backend!r} is not durable. "
            f"Production deployments require 'qdrant'. "
            f"Set VECTOR_STORE_BACKEND=qdrant."
        )
    if backend == "qdrant":
        return QdrantVectorStore(url=url, api_key=api_key, prefer_grpc=True)
    return InMemoryVectorStore()
