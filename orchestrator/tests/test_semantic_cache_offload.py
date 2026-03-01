from __future__ import annotations

import inspect
import time
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.semantic_cache import (
    CacheConfig,
    CacheEntry,
    InlineSimilarityBackend,
    SemanticQueryCache,
    ThreadPoolSimilarityBackend,
    _vectorized_best_match,
)


def _make_entry(
    embedding: list[float],
    query: str = "test",
    key_hash: str = "h1",
) -> CacheEntry:
    return CacheEntry(
        key_hash=key_hash,
        embedding=embedding,
        query=query,
        result={"answer": query},
        created_at=time.monotonic(),
        ttl_seconds=300.0,
    )


class TestThreadPoolSimilarityBackend:

    @pytest.mark.asyncio
    async def test_returns_same_result_as_vectorized_best_match(self) -> None:
        backend = ThreadPoolSimilarityBackend()
        query_embedding = [1.0, 0.0, 0.0]
        entries = [
            _make_entry([1.0, 0.0, 0.0], key_hash="h1"),
            _make_entry([0.0, 1.0, 0.0], key_hash="h2"),
            _make_entry([0.0, 0.0, 1.0], key_hash="h3"),
        ]
        expected = _vectorized_best_match(query_embedding, entries)
        result = await backend.find_best_match(query_embedding, entries)
        assert result[0] is expected[0]
        assert result[1] == pytest.approx(expected[1])
        assert result[2] == pytest.approx(expected[2])

    def test_find_best_match_is_coroutine_function(self) -> None:
        backend = ThreadPoolSimilarityBackend()
        assert inspect.iscoroutinefunction(backend.find_best_match)


class TestInlineSimilarityBackend:

    def test_find_best_match_is_coroutine_function(self) -> None:
        backend = InlineSimilarityBackend()
        assert inspect.iscoroutinefunction(backend.find_best_match)

    @pytest.mark.asyncio
    async def test_returns_same_result_as_vectorized_best_match(self) -> None:
        backend = InlineSimilarityBackend()
        query_embedding = [1.0, 0.0, 0.0]
        entries = [
            _make_entry([0.9, 0.1, 0.0], key_hash="h1"),
            _make_entry([0.0, 1.0, 0.0], key_hash="h2"),
        ]
        expected = _vectorized_best_match(query_embedding, entries)
        result = await backend.find_best_match(query_embedding, entries)
        assert result[0] is expected[0]
        assert result[1] == pytest.approx(expected[1])
        assert result[2] == pytest.approx(expected[2])


class TestSemanticQueryCacheBackendConfiguration:

    def test_uses_threadpool_backend_by_default(self) -> None:
        cache = SemanticQueryCache()
        assert isinstance(cache.similarity_backend, ThreadPoolSimilarityBackend)

    def test_configurable_with_inline_backend(self) -> None:
        backend = InlineSimilarityBackend()
        cache = SemanticQueryCache(similarity_backend=backend)
        assert cache.similarity_backend is backend

    @pytest.mark.asyncio
    async def test_async_lookup_delegates_to_backend(self) -> None:
        mock_backend = AsyncMock()
        mock_backend.find_best_match = AsyncMock(
            return_value=(None, -1.0, -1.0),
        )
        cache = SemanticQueryCache(
            config=CacheConfig(similarity_threshold=0.9),
            similarity_backend=mock_backend,
        )
        cache.store("q", [1.0, 0.0, 0.0], {"answer": "test"})
        await cache.async_lookup([1.0, 0.0, 0.0])
        mock_backend.find_best_match.assert_called_once()


class TestBackendEmptyEntries:

    @pytest.mark.asyncio
    async def test_threadpool_empty_entries(self) -> None:
        backend = ThreadPoolSimilarityBackend()
        result = await backend.find_best_match([1.0, 0.0], [])
        assert result[0] is None
        assert result[1] == -1.0
        assert result[2] == -1.0

    @pytest.mark.asyncio
    async def test_inline_empty_entries(self) -> None:
        backend = InlineSimilarityBackend()
        result = await backend.find_best_match([1.0, 0.0], [])
        assert result[0] is None
        assert result[1] == -1.0
        assert result[2] == -1.0


class TestAsyncLookupRespectsThreshold:

    @pytest.mark.asyncio
    async def test_rejects_below_threshold(self) -> None:
        cache = SemanticQueryCache(
            config=CacheConfig(similarity_threshold=0.99),
            similarity_backend=InlineSimilarityBackend(),
        )
        cache.store("q", [1.0, 0.0, 0.0], {"answer": "stored"})
        result = await cache.async_lookup([0.0, 1.0, 0.0])
        assert result is None

    @pytest.mark.asyncio
    async def test_accepts_above_threshold(self) -> None:
        cache = SemanticQueryCache(
            config=CacheConfig(similarity_threshold=0.9),
            similarity_backend=InlineSimilarityBackend(),
        )
        cache.store("q", [1.0, 0.0, 0.0], {"answer": "stored"})
        result = await cache.async_lookup([1.0, 0.0, 0.0])
        assert result == {"answer": "stored"}
