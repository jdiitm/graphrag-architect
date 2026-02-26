import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.semantic_cache import (
    CacheConfig,
    RedisSemanticQueryCache,
    SemanticQueryCache,
    create_semantic_cache,
)


@pytest.fixture
def mock_redis():
    r = AsyncMock()
    r.get = AsyncMock(return_value=None)
    r.setex = AsyncMock()
    r.delete = AsyncMock()
    r.scan = AsyncMock(return_value=(0, []))
    return r


@pytest.fixture
def redis_cache(mock_redis):
    with patch(
        "orchestrator.app.semantic_cache.create_async_redis",
        return_value=mock_redis,
    ):
        cache = RedisSemanticQueryCache(redis_url="redis://localhost:6379")
    cache._redis = mock_redis
    return cache


def _make_embedding(seed: float = 1.0) -> list:
    return [seed * 0.1] * 32


def test_store_and_lookup_roundtrip(redis_cache, mock_redis):
    loop = asyncio.new_event_loop()
    emb = _make_embedding(1.0)

    async def _run():
        redis_cache.store(
            query="what services depend on auth?",
            query_embedding=emb,
            result={"answer": "payment, user"},
            tenant_id="acme",
        )
        result = redis_cache.lookup(emb, tenant_id="acme")
        assert result is not None
        assert result["answer"] == "payment, user"

    loop.run_until_complete(_run())
    loop.close()


def test_tenant_isolation(redis_cache):
    emb = _make_embedding(2.0)
    redis_cache.store(
        query="q", query_embedding=emb,
        result={"answer": "a"}, tenant_id="acme",
    )
    result = redis_cache.lookup(emb, tenant_id="other-corp")
    assert result is None


@pytest.mark.asyncio
async def test_invalidate_tenant(redis_cache):
    emb = _make_embedding(3.0)
    redis_cache.store(
        query="q", query_embedding=emb,
        result={"answer": "a"}, tenant_id="acme",
    )
    count = await redis_cache.invalidate_tenant("acme")
    assert count == 1
    assert redis_cache.lookup(emb, tenant_id="acme") is None


def test_invalidate_all(redis_cache, mock_redis):
    loop = asyncio.new_event_loop()

    async def _run():
        emb = _make_embedding(4.0)
        redis_cache.store(
            query="q", query_embedding=emb,
            result={"answer": "a"}, tenant_id="acme",
        )
        await redis_cache.invalidate_all()
        assert redis_cache.lookup(emb, tenant_id="acme") is None

    loop.run_until_complete(_run())
    loop.close()


def test_singleflight_lookup_or_wait(redis_cache):
    loop = asyncio.new_event_loop()
    emb = _make_embedding(5.0)

    async def _run():
        result, is_owner = await redis_cache.lookup_or_wait(emb, tenant_id="t")
        assert result is None
        assert is_owner is True

        redis_cache.store(
            query="q", query_embedding=emb,
            result={"answer": "done"}, tenant_id="t",
        )
        redis_cache.notify_complete(emb)

    loop.run_until_complete(_run())
    loop.close()


def test_factory_returns_redis_when_url_set():
    with patch.dict("os.environ", {"REDIS_URL": "redis://localhost:6379"}):
        cache = create_semantic_cache()
    assert isinstance(cache, RedisSemanticQueryCache)


def test_factory_returns_local_when_no_url():
    with patch.dict("os.environ", {"REDIS_URL": ""}, clear=False):
        cache = create_semantic_cache()
    assert isinstance(cache, SemanticQueryCache)
    assert not isinstance(cache, RedisSemanticQueryCache)
