import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.vector_store import (
    QdrantClientPool,
    PooledQdrantVectorStore,
    VectorRecord,
)


@pytest.fixture
def healthy_client():
    client = AsyncMock()
    client.get_collections = AsyncMock(return_value=MagicMock())
    return client


@pytest.fixture
def stale_client():
    client = AsyncMock()
    client.get_collections = AsyncMock(side_effect=ConnectionError("gone"))
    return client


@pytest.mark.asyncio
class TestLazyAcquire:
    async def test_acquire_returns_idle_client_without_health_check(
        self, healthy_client,
    ) -> None:
        pool = QdrantClientPool(max_size=2)
        pool._factory = lambda: healthy_client
        pool._idle.append(healthy_client)

        client = await pool.acquire()

        assert client is healthy_client
        healthy_client.get_collections.assert_not_called()

    async def test_acquire_creates_new_when_idle_empty(self) -> None:
        sentinel = object()
        pool = QdrantClientPool(max_size=2)
        pool._factory = lambda: sentinel

        client = await pool.acquire()

        assert client is sentinel
        assert pool.stats()["active"] == 1

    async def test_acquire_release_roundtrip_preserves_counts(
        self, healthy_client,
    ) -> None:
        pool = QdrantClientPool(max_size=2)
        pool._factory = lambda: healthy_client

        client = await pool.acquire()
        assert pool.stats()["active"] == 1
        await pool.release(client)
        assert pool.stats()["active"] == 0
        assert pool.stats()["idle"] == 1


@pytest.mark.asyncio
class TestDiscard:
    async def test_discard_does_not_return_to_idle(self) -> None:
        sentinel = object()
        pool = QdrantClientPool(max_size=2)
        pool._factory = lambda: sentinel

        client = await pool.acquire()
        assert pool.stats()["active"] == 1

        await pool.discard(client)

        assert pool.stats()["active"] == 0
        assert pool.stats()["idle"] == 0

    async def test_discard_releases_semaphore_for_new_acquire(self) -> None:
        created = []

        def _factory():
            obj = object()
            created.append(obj)
            return obj

        pool = QdrantClientPool(max_size=1)
        pool._factory = _factory

        first = await pool.acquire()
        await pool.discard(first)
        second = await pool.acquire()

        assert second is not first
        assert len(created) == 2


@pytest.mark.asyncio
class TestPooledStoreDiscardsOnFailure:
    async def test_upsert_discards_client_on_exception(self) -> None:
        store = PooledQdrantVectorStore(url="http://localhost:6333", pool_size=1)
        failing_client = AsyncMock()
        failing_client.upsert = AsyncMock(side_effect=ConnectionError("broken"))
        store._pool._factory = lambda: failing_client
        store._pool.discard = AsyncMock()
        store._pool.release = AsyncMock()

        with pytest.raises(ConnectionError, match="broken"):
            await store.upsert("coll", [VectorRecord(id="1", vector=[0.1], metadata={})])

        store._pool.discard.assert_awaited_once_with(failing_client)
        store._pool.release.assert_not_awaited()

    async def test_search_discards_client_on_exception(self) -> None:
        store = PooledQdrantVectorStore(url="http://localhost:6333", pool_size=1)
        failing_client = AsyncMock()
        failing_client.search = AsyncMock(side_effect=ConnectionError("broken"))
        store._pool._factory = lambda: failing_client
        store._pool.discard = AsyncMock()
        store._pool.release = AsyncMock()

        with pytest.raises(ConnectionError, match="broken"):
            await store.search("coll", [0.1], limit=5)

        store._pool.discard.assert_awaited_once_with(failing_client)
        store._pool.release.assert_not_awaited()

    async def test_delete_discards_client_on_exception(self) -> None:
        store = PooledQdrantVectorStore(url="http://localhost:6333", pool_size=1)
        failing_client = AsyncMock()
        failing_client.delete = AsyncMock(side_effect=ConnectionError("broken"))
        store._pool._factory = lambda: failing_client
        store._pool.discard = AsyncMock()
        store._pool.release = AsyncMock()

        with pytest.raises(ConnectionError, match="broken"):
            await store.delete("coll", ["id1"])

        store._pool.discard.assert_awaited_once_with(failing_client)
        store._pool.release.assert_not_awaited()

    async def test_search_with_tenant_discards_client_on_exception(self) -> None:
        store = PooledQdrantVectorStore(url="http://localhost:6333", pool_size=1)
        failing_client = AsyncMock()
        failing_client.search = AsyncMock(side_effect=ConnectionError("broken"))
        store._pool._factory = lambda: failing_client
        store._pool.discard = AsyncMock()
        store._pool.release = AsyncMock()

        with pytest.raises(ConnectionError, match="broken"):
            await store.search_with_tenant("coll", [0.1], "tenant-1", limit=5)

        store._pool.discard.assert_awaited_once_with(failing_client)
        store._pool.release.assert_not_awaited()

    async def test_upsert_releases_client_on_success(self) -> None:
        store = PooledQdrantVectorStore(url="http://localhost:6333", pool_size=1)
        mock_client = AsyncMock()
        mock_client.upsert = AsyncMock()
        store._pool._factory = lambda: mock_client
        store._pool.discard = AsyncMock()
        store._pool.release = AsyncMock()

        await store.upsert("coll", [VectorRecord(id="1", vector=[0.1], metadata={})])

        store._pool.release.assert_awaited_once_with(mock_client)
        store._pool.discard.assert_not_awaited()


@pytest.mark.asyncio
class TestHealthSweep:
    async def test_sweep_removes_stale_clients_from_idle(
        self, stale_client,
    ) -> None:
        pool = QdrantClientPool(max_size=4)
        pool._idle.append(stale_client)

        removed = await pool.sweep_idle()

        assert removed == 1
        assert pool.stats()["idle"] == 0

    async def test_sweep_keeps_healthy_clients_in_idle(
        self, healthy_client,
    ) -> None:
        pool = QdrantClientPool(max_size=4)
        pool._idle.append(healthy_client)

        removed = await pool.sweep_idle()

        assert removed == 0
        assert pool.stats()["idle"] == 1

    async def test_sweep_mixed_idle_keeps_healthy_removes_stale(
        self, healthy_client, stale_client,
    ) -> None:
        pool = QdrantClientPool(max_size=4)
        pool._idle.append(healthy_client)
        pool._idle.append(stale_client)

        removed = await pool.sweep_idle()

        assert removed == 1
        assert pool.stats()["idle"] == 1

    async def test_start_health_sweep_creates_task_and_stop_cancels_it(self) -> None:
        pool = QdrantClientPool(max_size=4)
        assert pool._sweep_task is None

        pool.start_health_sweep(interval=60.0)
        assert pool._sweep_task is not None
        assert not pool._sweep_task.done()

        pool.stop_health_sweep()
        assert pool._sweep_task is None

    async def test_start_health_sweep_is_idempotent(self) -> None:
        pool = QdrantClientPool(max_size=4)
        pool.start_health_sweep(interval=60.0)
        first_task = pool._sweep_task

        pool.start_health_sweep(interval=60.0)
        assert pool._sweep_task is first_task

        pool.stop_health_sweep()

    async def test_stop_health_sweep_is_idempotent(self) -> None:
        pool = QdrantClientPool(max_size=2)
        pool.stop_health_sweep()
        pool.stop_health_sweep()


@pytest.mark.asyncio
class TestPoolMaxSizeValidation:
    async def test_pool_rejects_zero_max_size(self) -> None:
        with pytest.raises(ValueError, match="max_size must be >= 1"):
            QdrantClientPool(max_size=0)

    async def test_pool_rejects_negative_max_size(self) -> None:
        with pytest.raises(ValueError, match="max_size must be >= 1"):
            QdrantClientPool(max_size=-1)
