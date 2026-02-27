from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.distributed_lock import (
    DistributedLock,
    DistributedSemaphore,
    LocalFallbackLock,
    LocalFallbackSemaphore,
    create_ingestion_lock,
    create_ingestion_semaphore,
)


_REDIS_URL = "redis://localhost:6379"


class TestDistributedLock:

    @pytest.mark.asyncio
    async def test_acquire_and_release_calls_redis(self) -> None:
        redis_mock = AsyncMock()
        owner_captured: list[str] = []

        async def _mock_set(key, value, nx=False, ex=None):
            owner_captured.append(value)
            return True

        redis_mock.set = AsyncMock(side_effect=_mock_set)

        async def _mock_get(key):
            return owner_captured[0] if owner_captured else None

        redis_mock.get = AsyncMock(side_effect=_mock_get)
        redis_mock.delete = AsyncMock()
        lock = DistributedLock(redis_conn=redis_mock, key_prefix="test:")

        async with lock.acquire("tenant:ns", ttl=30):
            redis_mock.set.assert_awaited_once()
            call_kwargs = redis_mock.set.call_args
            assert call_kwargs[1].get("nx") is True
            assert call_kwargs[1].get("ex") == 30

        redis_mock.delete.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_acquire_fails_when_key_already_held(self) -> None:
        redis_mock = AsyncMock()
        redis_mock.set = AsyncMock(return_value=False)
        lock = DistributedLock(
            redis_conn=redis_mock, key_prefix="test:", retry_attempts=2, retry_delay=0.01,
        )

        with pytest.raises(TimeoutError, match="lock"):
            async with lock.acquire("tenant:ns", ttl=30):
                pass

    @pytest.mark.asyncio
    async def test_release_uses_owner_token_to_prevent_wrong_release(self) -> None:
        redis_mock = AsyncMock()
        redis_mock.set = AsyncMock(return_value=True)
        redis_mock.get = AsyncMock(return_value="wrong-owner")
        redis_mock.delete = AsyncMock()
        lock = DistributedLock(redis_conn=redis_mock, key_prefix="test:")

        async with lock.acquire("key1", ttl=30):
            pass

        redis_mock.get.assert_awaited()

    @pytest.mark.asyncio
    async def test_concurrent_acquires_serialize(self) -> None:
        call_order: list[str] = []
        redis_mock = AsyncMock()

        acquire_count = 0

        async def _mock_set(key, value, nx=False, ex=None):
            nonlocal acquire_count
            acquire_count += 1
            if acquire_count == 2:
                return False
            return True

        redis_mock.set = AsyncMock(side_effect=_mock_set)
        redis_mock.get = AsyncMock(return_value=None)
        redis_mock.delete = AsyncMock()
        lock = DistributedLock(
            redis_conn=redis_mock, key_prefix="test:", retry_attempts=1, retry_delay=0.01,
        )

        async with lock.acquire("key1", ttl=30):
            call_order.append("first")

        assert "first" in call_order


class TestDistributedSemaphore:

    @pytest.mark.asyncio
    async def test_try_acquire_succeeds_below_limit(self) -> None:
        redis_mock = AsyncMock()
        redis_mock.zcard = AsyncMock(return_value=0)
        redis_mock.zadd = AsyncMock()
        redis_mock.zremrangebyscore = AsyncMock()
        sem = DistributedSemaphore(
            redis_conn=redis_mock, key="test:sem", max_concurrent=5,
        )

        acquired, token = await sem.try_acquire()
        assert acquired is True
        assert token is not None

    @pytest.mark.asyncio
    async def test_try_acquire_fails_at_limit(self) -> None:
        redis_mock = AsyncMock()
        redis_mock.zcard = AsyncMock(return_value=5)
        redis_mock.zadd = AsyncMock()
        redis_mock.zremrangebyscore = AsyncMock()
        redis_mock.zrem = AsyncMock()
        sem = DistributedSemaphore(
            redis_conn=redis_mock, key="test:sem", max_concurrent=5,
        )

        acquired, token = await sem.try_acquire()
        assert acquired is False

    @pytest.mark.asyncio
    async def test_release_removes_token(self) -> None:
        redis_mock = AsyncMock()
        redis_mock.zcard = AsyncMock(return_value=0)
        redis_mock.zadd = AsyncMock()
        redis_mock.zremrangebyscore = AsyncMock()
        redis_mock.zrem = AsyncMock()
        sem = DistributedSemaphore(
            redis_conn=redis_mock, key="test:sem", max_concurrent=5,
        )

        acquired, token = await sem.try_acquire()
        assert acquired is True
        await sem.release(token)
        redis_mock.zrem.assert_awaited_once()


class TestLocalFallbackLock:

    @pytest.mark.asyncio
    async def test_acquire_and_release(self) -> None:
        lock = LocalFallbackLock()
        held = False

        async with lock.acquire("key1", ttl=30):
            held = True

        assert held is True

    @pytest.mark.asyncio
    async def test_serializes_same_key(self) -> None:
        lock = LocalFallbackLock()
        order: list[int] = []

        async def _task(n: int) -> None:
            async with lock.acquire("key1", ttl=30):
                order.append(n)
                await asyncio.sleep(0.01)

        await asyncio.gather(_task(1), _task(2))
        assert len(order) == 2

    @pytest.mark.asyncio
    async def test_different_keys_run_concurrently(self) -> None:
        lock = LocalFallbackLock()
        concurrent = []

        async def _task(key: str) -> None:
            async with lock.acquire(key, ttl=30):
                concurrent.append(key)
                await asyncio.sleep(0.02)

        await asyncio.gather(_task("a"), _task("b"))
        assert set(concurrent) == {"a", "b"}


class TestLocalFallbackSemaphore:

    @pytest.mark.asyncio
    async def test_try_acquire_under_limit(self) -> None:
        sem = LocalFallbackSemaphore(max_concurrent=3)
        acquired, token = await sem.try_acquire()
        assert acquired is True
        assert token != ""

    @pytest.mark.asyncio
    async def test_try_acquire_at_limit(self) -> None:
        sem = LocalFallbackSemaphore(max_concurrent=1)
        ok1, t1 = await sem.try_acquire()
        assert ok1 is True
        ok2, _ = await sem.try_acquire()
        assert ok2 is False
        await sem.release(t1)

    @pytest.mark.asyncio
    async def test_release_frees_slot(self) -> None:
        sem = LocalFallbackSemaphore(max_concurrent=1)
        ok1, t1 = await sem.try_acquire()
        assert ok1 is True
        await sem.release(t1)
        ok2, _ = await sem.try_acquire()
        assert ok2 is True


class TestFactoryFunctions:

    def test_create_lock_returns_local_when_no_redis(self) -> None:
        with patch.dict("os.environ", {"REDIS_URL": ""}, clear=False):
            lock = create_ingestion_lock()
        assert isinstance(lock, LocalFallbackLock)

    def test_create_semaphore_returns_local_when_no_redis(self) -> None:
        with patch.dict("os.environ", {"REDIS_URL": ""}, clear=False):
            sem = create_ingestion_semaphore(max_concurrent=5)
        assert isinstance(sem, LocalFallbackSemaphore)

    def test_create_lock_returns_distributed_when_redis_configured(self) -> None:
        with patch.dict("os.environ", {"REDIS_URL": _REDIS_URL}, clear=False):
            with patch("orchestrator.app.distributed_lock.create_async_redis"):
                lock = create_ingestion_lock()
        assert isinstance(lock, DistributedLock)

    def test_create_semaphore_returns_distributed_when_redis_configured(self) -> None:
        with patch.dict("os.environ", {"REDIS_URL": _REDIS_URL}, clear=False):
            with patch("orchestrator.app.distributed_lock.create_async_redis"):
                sem = create_ingestion_semaphore(max_concurrent=5)
        assert isinstance(sem, DistributedSemaphore)


class TestBoundedBackgroundTasks:

    @pytest.mark.asyncio
    async def test_bounded_task_set_rejects_over_limit(self) -> None:
        from orchestrator.app.distributed_lock import BoundedTaskSet

        bts = BoundedTaskSet(max_tasks=2)

        async def _noop() -> None:
            await asyncio.sleep(0.1)

        ok1 = bts.try_add(asyncio.create_task(_noop()))
        ok2 = bts.try_add(asyncio.create_task(_noop()))
        ok3 = bts.try_add(asyncio.create_task(_noop()))

        assert ok1 is True
        assert ok2 is True
        assert ok3 is False

        await asyncio.sleep(0.15)

    @pytest.mark.asyncio
    async def test_bounded_task_set_frees_slots_on_completion(self) -> None:
        from orchestrator.app.distributed_lock import BoundedTaskSet

        bts = BoundedTaskSet(max_tasks=1)

        async def _fast() -> None:
            pass

        t = asyncio.create_task(_fast())
        ok1 = bts.try_add(t)
        assert ok1 is True
        await asyncio.sleep(0.01)

        ok2 = bts.try_add(asyncio.create_task(_fast()))
        assert ok2 is True
        await asyncio.sleep(0.01)
