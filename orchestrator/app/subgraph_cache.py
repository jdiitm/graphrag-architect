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


def _prev_non_ws(tokens: list, idx: int) -> "CypherToken | None":
    for i in range(idx - 1, -1, -1):
        if tokens[i].token_type.value != "whitespace":
            return tokens[i]
    return None


def _next_non_ws(tokens: list, idx: int) -> "CypherToken | None":
    for i in range(idx + 1, len(tokens)):
        if tokens[i].token_type.value != "whitespace":
            return tokens[i]
    return None


def _is_variable_position(tokens: list, idx: int) -> bool:
    from orchestrator.app.cypher_tokenizer import TokenType

    tok = tokens[idx]
    prev = _prev_non_ws(tokens, idx)
    nxt = _next_non_ws(tokens, idx)

    if (
        prev is not None
        and prev.token_type == TokenType.PUNCTUATION
        and prev.value == ":"
        and prev.brace_depth == 0
    ):
        return False

    if (
        prev is not None
        and prev.token_type == TokenType.PUNCTUATION
        and prev.value == "."
    ):
        return False

    if (
        tok.brace_depth > 0
        and nxt is not None
        and nxt.token_type == TokenType.PUNCTUATION
        and nxt.value == ":"
    ):
        return False

    if (
        nxt is not None
        and nxt.token_type == TokenType.PUNCTUATION
        and nxt.value == "("
    ):
        return False

    return True


def _normalize_variable_aliases(cypher: str) -> str:
    from orchestrator.app.cypher_tokenizer import TokenType, tokenize_cypher

    tokens = tokenize_cypher(cypher)
    alias_map: dict[str, str] = {}
    counter = 0
    result_parts: list[str] = []

    for i, tok in enumerate(tokens):
        if tok.token_type == TokenType.IDENTIFIER and _is_variable_position(tokens, i):
            lowered = tok.value.lower()
            if lowered not in alias_map:
                alias_map[lowered] = f"_v{counter}"
                counter += 1
            result_parts.append(alias_map[lowered])
        else:
            result_parts.append(tok.value)

    return "".join(result_parts)


def normalize_cypher(query: str) -> str:
    aliased = _normalize_variable_aliases(query)
    normalized = aliased.lower().strip()
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
        self._generations: Dict[str, int] = {}
        self._generation = 0
        self._hits = 0
        self._misses = 0

    @property
    def generation(self) -> int:
        return self._generation

    def advance_generation(self) -> int:
        self._generation += 1
        return self._generation

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
                evicted_key, _ = self._cache.popitem(last=False)
                self._generations.pop(evicted_key, None)
        self._cache[key] = value
        self._generations[key] = self._generation

    def invalidate(self, key: str) -> None:
        if key in self._cache:
            del self._cache[key]
            self._generations.pop(key, None)

    def invalidate_stale(self) -> int:
        stale = [
            k for k, gen in self._generations.items()
            if gen < self._generation
        ]
        for k in stale:
            self._cache.pop(k, None)
            self._generations.pop(k, None)
        return len(stale)

    def invalidate_all(self) -> None:
        self._cache.clear()
        self._generations.clear()

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

    @property
    def generation(self) -> int:
        return self._l1.generation

    def advance_generation(self) -> int:
        return self._l1.advance_generation()

    def invalidate_stale(self) -> int:
        return self._l1.invalidate_stale()

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
            cursor = None
            pattern = f"{self._prefix}*"
            while cursor != 0:
                cursor, keys = await self._redis.scan(
                    cursor=cursor or 0, match=pattern, count=100,
                )
                if keys:
                    await self._redis.delete(*keys)
        except Exception:
            logger.debug("Redis prefix-scoped invalidation failed during invalidate_all")

    def stats(self) -> CacheStats:
        return self._l1.stats()
