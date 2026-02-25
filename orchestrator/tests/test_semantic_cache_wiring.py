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
