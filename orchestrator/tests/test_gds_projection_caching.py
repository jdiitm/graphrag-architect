from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.graph_embeddings import GDSProjectionManager


class FakeRedis:
    def __init__(self):
        self._store: dict = {}

    async def get(self, key: str):
        return self._store.get(key)

    async def set(self, key: str, value: str):
        self._store[key] = value

    async def delete(self, key: str):
        self._store.pop(key, None)


@pytest.fixture
def fake_redis():
    return FakeRedis()


@pytest.fixture
def mock_driver():
    driver = MagicMock()
    session = AsyncMock()
    session.execute_write = AsyncMock(return_value=None)
    session.execute_read = AsyncMock(return_value=None)

    async def _session_ctx(**kwargs):
        return session

    driver.session = MagicMock(return_value=session)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    return driver


class TestGDSProjectionManager:
    @pytest.mark.asyncio
    async def test_ensure_projection_creates_on_first_call(
        self, fake_redis, mock_driver,
    ):
        manager = GDSProjectionManager(
            driver=mock_driver, redis_conn=fake_redis,
        )
        created = await manager.ensure_projection("tenant-a")
        assert created is True
        cached = await fake_redis.get("gds:projection:tenant-a:exists")
        assert cached is not None

    @pytest.mark.asyncio
    async def test_ensure_projection_reuses_cached(
        self, fake_redis, mock_driver,
    ):
        manager = GDSProjectionManager(
            driver=mock_driver, redis_conn=fake_redis,
        )
        await manager.ensure_projection("tenant-a")
        first_call_count = mock_driver.session.call_count

        created = await manager.ensure_projection("tenant-a")
        assert created is False
        assert mock_driver.session.call_count == first_call_count

    @pytest.mark.asyncio
    async def test_invalidate_drops_projection(
        self, fake_redis, mock_driver,
    ):
        manager = GDSProjectionManager(
            driver=mock_driver, redis_conn=fake_redis,
        )
        await manager.ensure_projection("tenant-b")
        await manager.invalidate_projection("tenant-b")

        stale = await fake_redis.get("gds:projection:tenant-b:exists")
        assert stale is None

        created = await manager.ensure_projection("tenant-b")
        assert created is True

    @pytest.mark.asyncio
    async def test_tenant_isolation(self, fake_redis, mock_driver):
        manager = GDSProjectionManager(
            driver=mock_driver, redis_conn=fake_redis,
        )
        await manager.ensure_projection("tenant-a")
        await manager.ensure_projection("tenant-b")

        await manager.invalidate_projection("tenant-a")

        cached_a = await fake_redis.get("gds:projection:tenant-a:exists")
        cached_b = await fake_redis.get("gds:projection:tenant-b:exists")
        assert cached_a is None
        assert cached_b is not None

    @pytest.mark.asyncio
    async def test_stale_projection_recreated(
        self, fake_redis, mock_driver,
    ):
        manager = GDSProjectionManager(
            driver=mock_driver, redis_conn=fake_redis,
            mutation_threshold=2,
        )
        await manager.ensure_projection("tenant-c")

        manager.record_mutation("tenant-c")
        manager.record_mutation("tenant-c")

        created = await manager.ensure_projection("tenant-c")
        assert created is True

    def test_graph_name_is_tenant_scoped(self):
        manager = GDSProjectionManager(
            driver=MagicMock(), redis_conn=MagicMock(),
        )
        assert manager.graph_name("tenant-x") == "graphrag_tenant-x"
