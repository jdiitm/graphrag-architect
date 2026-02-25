import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.vector_store import QdrantClientPool


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


def test_healthy_connection_returned(healthy_client):
    loop = asyncio.new_event_loop()

    async def _run():
        pool = QdrantClientPool(max_size=2)
        pool._factory = lambda: healthy_client
        pool._idle.append(healthy_client)
        client = await pool.acquire()
        assert client is healthy_client

    loop.run_until_complete(_run())
    loop.close()


def test_stale_connection_discarded_and_new_created(stale_client, healthy_client):
    loop = asyncio.new_event_loop()
    created = []

    def _factory():
        created.append(1)
        return healthy_client

    async def _run():
        pool = QdrantClientPool(max_size=2)
        pool._factory = _factory
        pool._idle.append(stale_client)
        client = await pool.acquire()
        assert client is healthy_client
        assert len(created) == 1

    loop.run_until_complete(_run())
    loop.close()


def test_pool_stats_after_discard(stale_client, healthy_client):
    loop = asyncio.new_event_loop()

    async def _run():
        pool = QdrantClientPool(max_size=2)
        pool._factory = lambda: healthy_client
        pool._idle.append(stale_client)
        await pool.acquire()
        stats = pool.stats()
        assert stats["active"] == 1
        assert stats["idle"] == 0

    loop.run_until_complete(_run())
    loop.close()
