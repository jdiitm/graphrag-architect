from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Protocol, runtime_checkable


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
