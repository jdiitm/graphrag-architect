from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from collections import deque
from typing import Any, Dict, List

from orchestrator.app.redis_client import create_async_redis, require_redis

logger = logging.getLogger(__name__)

_DEFAULT_MAX_SIZE = 10_000
_DEFAULT_KEY_PREFIX = "graphrag:"


class ASTDeadLetterQueue(ABC):

    @abstractmethod
    def enqueue(self, payload: Dict[str, Any]) -> None: ...

    @abstractmethod
    def drain(self) -> List[Dict[str, Any]]: ...

    @abstractmethod
    def peek(self) -> List[Dict[str, Any]]: ...

    @abstractmethod
    def size(self) -> int: ...

    @abstractmethod
    def clear(self) -> None: ...


class InMemoryASTDLQ(ASTDeadLetterQueue):

    def __init__(self, max_size: int = _DEFAULT_MAX_SIZE) -> None:
        if max_size < 1:
            raise ValueError(f"max_size must be >= 1, got {max_size}")
        self._max_size = max_size
        self._buffer: deque[Dict[str, Any]] = deque(maxlen=max_size)

    def enqueue(self, payload: Dict[str, Any]) -> None:
        if len(self._buffer) >= self._max_size:
            logger.warning(
                "AST DLQ at capacity (%d); oldest entry evicted",
                self._max_size,
            )
        self._buffer.append(payload)

    def drain(self) -> List[Dict[str, Any]]:
        items = list(self._buffer)
        self._buffer.clear()
        return items

    def peek(self) -> List[Dict[str, Any]]:
        return list(self._buffer)

    def size(self) -> int:
        return len(self._buffer)

    def clear(self) -> None:
        self._buffer.clear()


_DRAIN_LUA = """
local items = redis.call('lrange', KEYS[1], 0, -1)
if #items > 0 then
    redis.call('del', KEYS[1])
end
return items
"""


class RedisASTDLQ(ASTDeadLetterQueue):
    _redis: Any
    _max_size: int
    _key_prefix: str
    _list_key: str
    _local: InMemoryASTDLQ

    def __init__(
        self,
        redis_url: str,
        max_size: int = _DEFAULT_MAX_SIZE,
        key_prefix: str = _DEFAULT_KEY_PREFIX,
        password: str = "",
        db: int = 0,
    ) -> None:
        require_redis("RedisASTDLQ")
        self._redis = create_async_redis(redis_url, password=password, db=db)
        self._max_size = max_size
        self._key_prefix = key_prefix
        self._list_key = f"{key_prefix}ast_dlq"
        self._local = InMemoryASTDLQ(max_size=max_size)

    def enqueue(self, payload: Dict[str, Any]) -> None:
        self._local.enqueue(payload)

    def drain(self) -> List[Dict[str, Any]]:
        return self._local.drain()

    def peek(self) -> List[Dict[str, Any]]:
        return self._local.peek()

    def size(self) -> int:
        return self._local.size()

    def clear(self) -> None:
        self._local.clear()

    async def async_enqueue(self, payload: Dict[str, Any]) -> None:
        raw = json.dumps(payload, default=str)
        await self._redis.rpush(self._list_key, raw)
        await self._redis.ltrim(self._list_key, -self._max_size, -1)
        self._local.enqueue(payload)

    async def async_drain(self) -> List[Dict[str, Any]]:
        raw_entries = await self._redis.eval(_DRAIN_LUA, 1, self._list_key)
        self._local.clear()
        if not raw_entries:
            return []
        return [json.loads(entry) for entry in raw_entries]

    async def async_peek(self) -> List[Dict[str, Any]]:
        raw_entries = await self._redis.lrange(self._list_key, 0, -1)
        return [json.loads(entry) for entry in raw_entries]

    async def async_size(self) -> int:
        return await self._redis.llen(self._list_key)

    async def async_clear(self) -> None:
        await self._redis.delete(self._list_key)
        self._local.clear()


def create_ast_dlq() -> ASTDeadLetterQueue:
    from orchestrator.app.config import RedisConfig
    redis_cfg = RedisConfig.from_env()
    max_size = int(os.environ.get("AST_DLQ_MAX_SIZE", str(_DEFAULT_MAX_SIZE)))
    if redis_cfg.url:
        return RedisASTDLQ(
            redis_url=redis_cfg.url,
            max_size=max_size,
            password=redis_cfg.password,
            db=redis_cfg.db,
        )
    return InMemoryASTDLQ(max_size=max_size)
