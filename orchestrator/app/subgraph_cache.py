from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class CacheStats:
    hits: int
    misses: int
    size: int
    maxsize: int


def normalize_cypher(query: str) -> str:
    normalized = query.lower().strip()
    normalized = " ".join(normalized.split())
    if normalized.endswith(";"):
        normalized = normalized[:-1].rstrip()
    return normalized


class SubgraphCache:
    def __init__(self, maxsize: int = 256) -> None:
        self._maxsize = maxsize
        self._cache: OrderedDict[str, List[Dict[str, Any]]] = OrderedDict()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[List[Dict[str, Any]]]:
        if key not in self._cache:
            self._misses += 1
            return None
        self._hits += 1
        self._cache.move_to_end(key)
        return self._cache[key]

    def put(self, key: str, value: List[Dict[str, Any]]) -> None:
        if key in self._cache:
            self._cache.move_to_end(key)
        else:
            while len(self._cache) >= self._maxsize and self._cache:
                self._cache.popitem(last=False)
        self._cache[key] = value

    def invalidate(self, key: str) -> None:
        if key in self._cache:
            del self._cache[key]

    def invalidate_all(self) -> None:
        self._cache.clear()

    def stats(self) -> CacheStats:
        return CacheStats(
            hits=self._hits,
            misses=self._misses,
            size=len(self._cache),
            maxsize=self._maxsize,
        )
