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

    async def delete(self, collection: str, ids: List[str]) -> int:
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

    async def delete(self, collection: str, ids: List[str]) -> int:
        if collection not in self._store:
            return 0
        removed = 0
        for record_id in ids:
            if record_id in self._store[collection]:
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


class Neo4jVectorStore:
    is_read_replica: bool = False

    def __init__(self, driver: Any = None) -> None:
        self._driver = driver

    def _get_driver(self) -> Any:
        if self._driver is None:
            from orchestrator.app.neo4j_pool import get_driver
            self._driver = get_driver()
        return self._driver

    async def upsert(
        self, collection: str, vectors: List[VectorRecord],
    ) -> int:
        async def _tx(tx: Any) -> int:
            for record in vectors:
                await tx.run(
                    "MERGE (n {id: $id}) "
                    "SET n.embedding = $vector, n += $metadata",
                    id=record.id,
                    vector=record.vector,
                    metadata=record.metadata,
                )
            return len(vectors)

        async with self._get_driver().session() as session:
            return await session.execute_write(_tx)

    async def search(
        self,
        collection: str,
        query_vector: List[float],
        limit: int = 10,
    ) -> List[SearchResult]:
        async def _tx(tx: Any) -> list:
            result = await tx.run(
                "CALL db.index.vector.queryNodes($index, $k, $vector) "
                "YIELD node, score "
                "RETURN node.id AS id, score, node {.*} AS metadata "
                "LIMIT $limit",
                index=collection,
                k=limit,
                vector=query_vector,
                limit=limit,
            )
            return await result.data()

        async with self._get_driver().session() as session:
            records = await session.execute_read(_tx)

        return [
            SearchResult(
                id=str(r["id"]),
                score=r["score"],
                metadata={k: v for k, v in r.get("metadata", {}).items() if k != "embedding"},
            )
            for r in records
        ]

    async def delete(self, collection: str, ids: List[str]) -> int:
        async def _tx(tx: Any) -> int:
            result = await tx.run(
                "MATCH (n) WHERE n.id IN $ids "
                "REMOVE n.embedding "
                "RETURN count(n) AS removed",
                ids=ids,
            )
            data = await result.data()
            return data[0]["removed"] if data else 0

        async with self._get_driver().session() as session:
            return await session.execute_write(_tx)

    async def search_with_tenant(
        self,
        collection: str,
        query_vector: List[float],
        tenant_id: str,
        limit: int = 10,
    ) -> List[SearchResult]:
        results = await self.search(collection, query_vector, limit=limit * 2)
        return [
            r for r in results
            if not tenant_id or r.metadata.get("tenant_id") == tenant_id
        ][:limit]


class QdrantVectorStore:
    is_read_replica: bool = True

    def __init__(
        self,
        url: str = "http://localhost:6333",
        api_key: str = "",
    ) -> None:
        self._url = url
        self._api_key = api_key
        self._client: Optional[Any] = None

    def _get_client(self) -> Any:
        if self._client is None:
            try:
                from qdrant_client import AsyncQdrantClient
                self._client = AsyncQdrantClient(
                    url=self._url,
                    api_key=self._api_key or None,
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

    async def delete(self, collection: str, ids: List[str]) -> int:
        client = self._get_client()
        from qdrant_client.models import PointIdsList
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
) -> Any:
    if backend == "neo4j":
        return Neo4jVectorStore(driver=driver)
    if backend == "qdrant":
        return QdrantVectorStore(url=url, api_key=api_key)
    return InMemoryVectorStore()
