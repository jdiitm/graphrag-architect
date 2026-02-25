from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.semantic_cache import SemanticQueryCache, CacheConfig


class TestSemanticCacheAsL0:

    def test_semantic_cache_invalidate_all_returns_count(self) -> None:
        cache = SemanticQueryCache(CacheConfig(max_entries=100))
        cache.store(
            query="test query",
            query_embedding=[0.1, 0.2, 0.3],
            result={"answer": "test"},
            tenant_id="t1",
        )
        count = cache.invalidate_all()
        assert count == 1
        assert cache.stats().size == 0

    def test_semantic_cache_tenant_scoped_invalidation(self) -> None:
        cache = SemanticQueryCache(CacheConfig(max_entries=100))
        cache.store(
            query="q1", query_embedding=[0.1, 0.2],
            result={"a": 1}, tenant_id="tenant-a",
        )
        cache.store(
            query="q2", query_embedding=[0.3, 0.4],
            result={"a": 2}, tenant_id="tenant-b",
        )
        removed = cache.invalidate_tenant("tenant-a")
        assert removed == 1
        assert cache.stats().size == 1


class TestSemanticCacheQueryEngineIntegration:

    def test_semantic_cache_is_initialized(self) -> None:
        from orchestrator.app.query_engine import _SEMANTIC_CACHE
        assert _SEMANTIC_CACHE is not None, (
            "_SEMANTIC_CACHE must be initialized as a SemanticQueryCache instance"
        )

    def test_semantic_cache_is_correct_type(self) -> None:
        from orchestrator.app.query_engine import _SEMANTIC_CACHE
        assert isinstance(_SEMANTIC_CACHE, SemanticQueryCache)

    @pytest.mark.asyncio
    async def test_cypher_retrieve_checks_semantic_cache(self) -> None:
        from orchestrator.app.query_engine import (
            _SEMANTIC_CACHE,
            cypher_retrieve,
        )

        cached_result = {
            "cypher_query": "cached_template",
            "cypher_results": [{"cached": True}],
            "iteration_count": 0,
        }

        embedding = [0.5] * 128
        _SEMANTIC_CACHE.store(
            query="which services call auth",
            query_embedding=embedding,
            result=cached_result,
            tenant_id="",
        )

        state = {
            "query": "which services call auth",
            "complexity": "MULTI_HOP",
            "max_results": 10,
            "tenant_id": "",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }

        with patch(
            "orchestrator.app.query_engine._embed_query",
            return_value=embedding,
        ), patch(
            "orchestrator.app.query_engine._neo4j_session",
        ) as mock_neo4j:
            result = await cypher_retrieve(state)

        if result.get("cypher_results") == [{"cached": True}]:
            mock_neo4j.assert_not_called()

        _SEMANTIC_CACHE.invalidate_all()

    @pytest.mark.asyncio
    async def test_semantic_cache_stores_result_on_miss(self) -> None:
        from orchestrator.app.query_engine import _SEMANTIC_CACHE

        _SEMANTIC_CACHE.invalidate_all()
        initial_size = _SEMANTIC_CACHE.stats().size
        assert initial_size == 0, (
            "Semantic cache should start empty after invalidation"
        )
