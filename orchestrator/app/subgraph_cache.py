from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from orchestrator.app.redis_client import create_async_redis, require_redis

logger = logging.getLogger(__name__)


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


def default_cache_maxsize() -> int:
    raw = os.environ.get("SUBGRAPH_CACHE_MAXSIZE", "256")
    try:
        return int(raw)
    except ValueError:
        return 256


def _estimate_bytes(value: List[Dict[str, Any]]) -> int:
    return sys.getsizeof(json.dumps(value, default=str))


class SubgraphCache:
    def __init__(
        self, maxsize: int = 256, max_value_bytes: int = 0,
    ) -> None:
        self._maxsize = maxsize
        self._max_value_bytes = max_value_bytes
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
        if self._max_value_bytes > 0:
            if _estimate_bytes(value) > self._max_value_bytes:
                return
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


def cache_key(cypher: str, acl_params: Dict[str, str]) -> str:
    normalized = normalize_cypher(cypher)
    sorted_acl = json.dumps(acl_params, sort_keys=True)
    raw = f"{normalized}|{sorted_acl}"
    return hashlib.sha256(raw.encode()).hexdigest()


def create_subgraph_cache() -> Any:
    from orchestrator.app.config import RedisConfig
    redis_cfg = RedisConfig.from_env()
    if redis_cfg.url:
        return RedisSubgraphCache(
            redis_url=redis_cfg.url,
            password=redis_cfg.password,
            db=redis_cfg.db,
            key_prefix=redis_cfg.key_prefix,
        )
    return SubgraphCache(maxsize=default_cache_maxsize())


class RedisSubgraphCache:
    def __init__(
        self,
        redis_url: str,
        ttl_seconds: int = 300,
        key_prefix: str = "graphrag:sgcache:",
        l1_maxsize: int = 256,
        password: str = "",
        db: int = 0,
    ) -> None:
        require_redis("RedisSubgraphCache")
        self._redis = create_async_redis(redis_url, password=password, db=db)
        self._ttl = ttl_seconds
        self._prefix = key_prefix
        self._l1 = SubgraphCache(maxsize=l1_maxsize)

    def _rkey(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def get_sync(self, key: str) -> Optional[List[Dict[str, Any]]]:
        return self._l1.get(key)

    async def get(self, key: str) -> Optional[List[Dict[str, Any]]]:
        l1_result = self._l1.get(key)
        if l1_result is not None:
            return l1_result
        try:
            raw = await self._redis.get(self._rkey(key))
            if raw is not None:
                value = json.loads(raw)
                self._l1.put(key, value)
                return value
        except Exception:
            logger.debug("Redis cache get failed, falling back to L1 miss")
        return None

    async def put(self, key: str, value: List[Dict[str, Any]]) -> None:
        self._l1.put(key, value)
        try:
            raw = json.dumps(value)
            await self._redis.setex(self._rkey(key), self._ttl, raw)
        except Exception:
            logger.debug("Redis cache put failed, L1 still updated")

    async def invalidate_all(self) -> None:
        self._l1.invalidate_all()
        try:
            await self._redis.flushdb()
        except Exception:
            logger.debug("Redis flushdb failed during invalidate_all")

    def stats(self) -> CacheStats:
        return self._l1.stats()
