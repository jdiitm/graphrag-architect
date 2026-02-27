from __future__ import annotations

import asyncio
import logging
import math
import warnings
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Protocol, runtime_checkable

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


class Neo4jVectorStore:
    is_read_replica: bool = False

    def __init__(self, driver: Any = None) -> None:
        warnings.warn(
            "Neo4jVectorStore is deprecated. Storing embeddings as Neo4j node "
            "properties pollutes the PageCache and prevents independent scaling. "
            "Migrate to QdrantVectorStore or PooledQdrantVectorStore by setting "
            "VECTOR_STORE_BACKEND=qdrant.",
            DeprecationWarning,
            stacklevel=2,
        )
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

    async def delete(self, collection: str, ids: List[str], tenant_id: str = "") -> int:
        async def _tx(tx: Any) -> int:
            if tenant_id:
                cypher = (
                    "MATCH (n) WHERE n.id IN $ids AND n.tenant_id = $tenant_id "
                    "REMOVE n.embedding "
                    "SET n.embedding_removed = true, "
                    "n.embedding_removed_at = datetime() "
                    "RETURN count(n) AS removed"
                )
                result = await tx.run(cypher, ids=ids, tenant_id=tenant_id)
            else:
                cypher = (
                    "MATCH (n) WHERE n.id IN $ids "
                    "REMOVE n.embedding "
                    "SET n.embedding_removed = true, "
                    "n.embedding_removed_at = datetime() "
                    "RETURN count(n) AS removed"
                )
                result = await tx.run(cypher, ids=ids)
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
        if not tenant_id:
            return await self.search(collection, query_vector, limit=limit)

        async def _tx(tx: Any) -> list:
            result = await tx.run(
                "CALL db.index.vector.queryNodes($index, $k, $vector) "
                "YIELD node, score "
                "WHERE node.tenant_id = $tenant_id "
                "RETURN node.id AS id, score, node {.*} AS metadata "
                "LIMIT $limit",
                index=collection,
                k=limit * 2,
                vector=query_vector,
                tenant_id=tenant_id,
                limit=limit,
            )
            return await result.data()

        async with self._get_driver().session() as session:
            records = await session.execute_read(_tx)

        return [
            SearchResult(
                id=str(r["id"]),
                score=r["score"],
                metadata={
                    k: v for k, v in r.get("metadata", {}).items()
                    if k != "embedding"
                },
            )
            for r in records
        ]


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


class QdrantClientPool:
    def __init__(
        self,
        max_size: int = 4,
        url: str = "http://localhost:6333",
        api_key: str = "",
    ) -> None:
        if max_size < 1:
            raise ValueError(
                f"max_size must be >= 1, got {max_size}"
            )
        self._max_size = max_size
        self._url = url
        self._api_key = api_key
        self._semaphore = asyncio.Semaphore(max_size)
        self._idle: deque[Any] = deque()
        self._active_count = 0
        self._factory: Callable[[], Any] = self._default_factory
        self._sweep_task: Optional[asyncio.Task[None]] = None

    def _default_factory(self) -> Any:
        try:
            from qdrant_client import AsyncQdrantClient
            return AsyncQdrantClient(
                url=self._url,
                api_key=self._api_key or None,
            )
        except ImportError as exc:
            raise RuntimeError(
                "qdrant-client is required for QdrantClientPool"
            ) from exc

    @property
    def max_size(self) -> int:
        return self._max_size

    async def _health_check(self, client: Any) -> bool:
        try:
            await client.get_collections()
            return True
        except Exception:
            return False

    async def acquire(self) -> Any:
        await self._semaphore.acquire()
        if self._idle:
            client = self._idle.popleft()
            self._active_count += 1
            return client
        client = self._factory()
        self._active_count += 1
        return client

    async def release(self, client: Any) -> None:
        self._active_count -= 1
        self._idle.append(client)
        self._semaphore.release()

    async def discard(self, client: Any) -> None:
        _ = client
        self._active_count -= 1
        self._semaphore.release()

    async def sweep_idle(self) -> int:
        healthy: deque[Any] = deque()
        removed = 0
        while self._idle:
            client = self._idle.popleft()
            if await self._health_check(client):
                healthy.append(client)
            else:
                removed += 1
                logger.debug("Background sweep: discarded stale Qdrant connection")
        self._idle = healthy
        return removed

    def start_health_sweep(self, interval: float = 30.0) -> None:
        if self._sweep_task is not None:
            return

        async def _loop() -> None:
            while True:
                await asyncio.sleep(interval)
                try:
                    await self.sweep_idle()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.debug("Health sweep iteration failed", exc_info=True)

        self._sweep_task = asyncio.get_event_loop().create_task(_loop())

    def stop_health_sweep(self) -> None:
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            self._sweep_task = None

    def stats(self) -> Dict[str, int]:
        return {
            "active": self._active_count,
            "idle": len(self._idle),
            "max_size": self._max_size,
        }


class PooledQdrantVectorStore:
    is_read_replica: bool = True

    def __init__(
        self,
        url: str = "http://localhost:6333",
        api_key: str = "",
        pool_size: int = 4,
    ) -> None:
        self._pool = QdrantClientPool(
            max_size=pool_size, url=url, api_key=api_key,
        )

    async def upsert(
        self, collection: str, vectors: List[VectorRecord],
    ) -> int:
        client = await self._pool.acquire()
        try:
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
        finally:
            await self._pool.release(client)

    async def search(
        self,
        collection: str,
        query_vector: List[float],
        limit: int = 10,
    ) -> List[SearchResult]:
        client = await self._pool.acquire()
        try:
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
        finally:
            await self._pool.release(client)

    async def delete(self, collection: str, ids: List[str], tenant_id: str = "") -> int:
        client = await self._pool.acquire()
        try:
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
        finally:
            await self._pool.release(client)

    async def search_with_tenant(
        self,
        collection: str,
        query_vector: List[float],
        tenant_id: str,
        limit: int = 10,
    ) -> List[SearchResult]:
        client = await self._pool.acquire()
        try:
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
        finally:
            await self._pool.release(client)


def create_vector_store(
    backend: str = "memory",
    url: str = "",
    api_key: str = "",
    driver: Any = None,
    pool_size: int = 4,
    deployment_mode: str = "dev",
) -> Any:
    if deployment_mode == "production" and backend not in ("qdrant", "neo4j"):
        raise ValueError(
            f"VECTOR_STORE_BACKEND={backend!r} is not durable. "
            f"Production deployments require 'qdrant' (recommended) or "
            f"'neo4j' (deprecated). Set VECTOR_STORE_BACKEND=qdrant."
        )
    if backend == "neo4j":
        return Neo4jVectorStore(driver=driver)
    if backend == "qdrant":
        return PooledQdrantVectorStore(
            url=url, api_key=api_key, pool_size=pool_size,
        )
    return InMemoryVectorStore()
