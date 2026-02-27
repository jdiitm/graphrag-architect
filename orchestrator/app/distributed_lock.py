from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Iterator, Tuple

from orchestrator.app.redis_client import create_async_redis

logger = logging.getLogger(__name__)

_RELEASE_LOCK_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

_SEMAPHORE_ACQUIRE_LUA = """
redis.call("zremrangebyscore", KEYS[1], "-inf", ARGV[1])
local count = redis.call("zcard", KEYS[1])
if count < tonumber(ARGV[2]) then
    redis.call("zadd", KEYS[1], ARGV[3], ARGV[4])
    return 1
end
return 0
"""

_DEFAULT_TTL = 300
_DEFAULT_RETRY_ATTEMPTS = 10
_DEFAULT_RETRY_DELAY = 0.3


class DistributedLock:
    def __init__(
        self,
        redis_conn: Any,
        key_prefix: str = "graphrag:lock:",
        retry_attempts: int = _DEFAULT_RETRY_ATTEMPTS,
        retry_delay: float = _DEFAULT_RETRY_DELAY,
    ) -> None:
        self._redis = redis_conn
        self._prefix = key_prefix
        self._retry_attempts = retry_attempts
        self._retry_delay = retry_delay

    @asynccontextmanager
    async def acquire(
        self, key: str, ttl: int = _DEFAULT_TTL,
    ) -> AsyncIterator[None]:
        full_key = f"{self._prefix}{key}"
        owner = uuid.uuid4().hex
        acquired = False
        for _ in range(self._retry_attempts):
            result = await self._redis.set(full_key, owner, nx=True, ex=ttl)
            if result:
                acquired = True
                break
            await asyncio.sleep(self._retry_delay)

        if not acquired:
            raise TimeoutError(
                f"Could not acquire distributed lock for key={key} "
                f"after {self._retry_attempts} attempts"
            )
        try:
            yield
        finally:
            released = await self._redis.eval(
                _RELEASE_LOCK_LUA, 1, full_key, owner,
            )
            if not released:
                logger.warning(
                    "Lock for %s was overwritten by another owner; skipping delete",
                    full_key,
                )


class DistributedSemaphore:
    def __init__(
        self,
        redis_conn: Any,
        key: str = "graphrag:ingest_sem",
        max_concurrent: int = 10,
        ttl: int = 600,
    ) -> None:
        self._redis = redis_conn
        self._key = key
        self._max = max_concurrent
        self._ttl = ttl

    async def try_acquire(self) -> Tuple[bool, str]:
        token = uuid.uuid4().hex
        now = time.time()
        cutoff = now - self._ttl
        result = await self._redis.eval(
            _SEMAPHORE_ACQUIRE_LUA, 1,
            self._key, str(cutoff), str(self._max), str(now), token,
        )
        if result == 1:
            return True, token
        return False, ""

    async def release(self, token: str) -> None:
        if token:
            await self._redis.zrem(self._key, token)


class LocalFallbackLock:
    def __init__(self) -> None:
        self._locks: Dict[str, asyncio.Lock] = {}
        self._refcounts: Dict[str, int] = {}
        self._guard = asyncio.Lock()

    @asynccontextmanager
    async def acquire(
        self, key: str, ttl: int = _DEFAULT_TTL,
    ) -> AsyncIterator[None]:
        async with self._guard:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
                self._refcounts[key] = 0
            self._refcounts[key] += 1
            lock = self._locks[key]
        try:
            async with lock:
                yield
        finally:
            async with self._guard:
                self._refcounts[key] -= 1
                if self._refcounts[key] <= 0:
                    del self._locks[key]
                    del self._refcounts[key]


class LocalFallbackSemaphore:
    def __init__(self, max_concurrent: int = 10) -> None:
        self._max = max_concurrent
        self._active: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def try_acquire(self) -> Tuple[bool, str]:
        async with self._lock:
            if len(self._active) >= self._max:
                return False, ""
            token = uuid.uuid4().hex
            self._active[token] = time.time()
            return True, token

    async def release(self, token: str) -> None:
        async with self._lock:
            self._active.pop(token, None)

    @property
    def active_count(self) -> int:
        return len(self._active)


class BoundedTaskSet:
    def __init__(self, max_tasks: int = 50) -> None:
        self._max = max_tasks
        self._tasks: set[asyncio.Task[Any]] = set()

    def try_add(self, task: asyncio.Task[Any]) -> bool:
        self._tasks = {t for t in self._tasks if not t.done()}
        if len(self._tasks) >= self._max:
            task.cancel()
            return False
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return True

    @property
    def active_count(self) -> int:
        self._tasks = {t for t in self._tasks if not t.done()}
        return len(self._tasks)

    def __iter__(self) -> Iterator[asyncio.Task[Any]]:
        self._tasks = {t for t in self._tasks if not t.done()}
        return iter(self._tasks)

    def __bool__(self) -> bool:
        self._tasks = {t for t in self._tasks if not t.done()}
        return len(self._tasks) > 0


def create_ingestion_lock(
    key_prefix: str = "graphrag:lock:",
) -> DistributedLock | LocalFallbackLock:
    from orchestrator.app.config import RedisConfig
    redis_cfg = RedisConfig.from_env()
    if redis_cfg.url:
        conn = create_async_redis(
            redis_cfg.url, password=redis_cfg.password, db=redis_cfg.db,
        )
        return DistributedLock(redis_conn=conn, key_prefix=key_prefix)
    return LocalFallbackLock()


def create_ingestion_semaphore(
    max_concurrent: int = 10,
    key: str = "graphrag:ingest_sem",
) -> DistributedSemaphore | LocalFallbackSemaphore:
    from orchestrator.app.config import RedisConfig
    redis_cfg = RedisConfig.from_env()
    if redis_cfg.url:
        conn = create_async_redis(
            redis_cfg.url, password=redis_cfg.password, db=redis_cfg.db,
        )
        return DistributedSemaphore(
            redis_conn=conn, key=key, max_concurrent=max_concurrent,
        )
    return LocalFallbackSemaphore(max_concurrent=max_concurrent)
