from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch, call

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
            acl_key="admin",
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
            "orchestrator.app.query_engine._build_traversal_acl_params",
            return_value={"is_admin": True, "acl_team": "", "acl_namespaces": []},
        ), patch(
            "orchestrator.app.query_engine._neo4j_session",
        ) as mock_neo4j:
            result = await cypher_retrieve(state)

        assert result.get("cypher_results") == [{"cached": True}], (
            "Expected cached result from semantic cache but got: "
            f"{result.get('cypher_results')!r}"
        )
        mock_neo4j.assert_not_called()

        _SEMANTIC_CACHE.invalidate_all()

    @pytest.mark.asyncio
    async def test_singleflight_notify_complete_on_success(self) -> None:
        from orchestrator.app.query_engine import (
            _SEMANTIC_CACHE,
            cypher_retrieve,
        )

        _SEMANTIC_CACHE.invalidate_all()
        embedding = [0.7] * 128

        mock_driver = AsyncMock()
        mock_session = AsyncMock()
        mock_driver.session.return_value.__aenter__ = AsyncMock(
            return_value=mock_session,
        )
        mock_driver.session.return_value.__aexit__ = AsyncMock(
            return_value=False,
        )

        state = {
            "query": "singleflight success test",
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
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._fetch_candidates",
            return_value=[{"name": "svc", "id": "svc-1"}],
        ), patch(
            "orchestrator.app.query_engine.run_traversal",
            return_value=[{"source": "svc", "rel": "CALLS", "target": "db"}],
        ), patch(
            "orchestrator.app.query_engine._build_traversal_acl_params",
            return_value={"is_admin": True, "acl_team": "", "acl_namespaces": []},
        ), patch.object(
            _SEMANTIC_CACHE, "notify_complete", wraps=_SEMANTIC_CACHE.notify_complete,
        ) as mock_notify:
            await cypher_retrieve(state)

        mock_notify.assert_called_once_with(embedding, acl_key="admin")
        _SEMANTIC_CACHE.invalidate_all()

    @pytest.mark.asyncio
    async def test_singleflight_notify_failed_on_error(self) -> None:
        from orchestrator.app.query_engine import (
            _SEMANTIC_CACHE,
            cypher_retrieve,
        )

        _SEMANTIC_CACHE.invalidate_all()
        embedding = [0.6] * 128

        state = {
            "query": "singleflight failure test",
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
            "orchestrator.app.query_engine._build_traversal_acl_params",
            return_value={"is_admin": True, "acl_team": "", "acl_namespaces": []},
        ), patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            side_effect=RuntimeError("db down"),
        ), patch.object(
            _SEMANTIC_CACHE, "notify_complete", wraps=_SEMANTIC_CACHE.notify_complete,
        ) as mock_notify:
            with pytest.raises(RuntimeError, match="db down"):
                await cypher_retrieve(state)

        mock_notify.assert_called_once_with(embedding, failed=True, acl_key="admin")
        _SEMANTIC_CACHE.invalidate_all()

    @pytest.mark.asyncio
    async def test_semantic_cache_stores_result_on_miss(self) -> None:
        from orchestrator.app.query_engine import (
            _SEMANTIC_CACHE,
            cypher_retrieve,
        )

        _SEMANTIC_CACHE.invalidate_all()
        assert _SEMANTIC_CACHE.stats().size == 0, (
            "Semantic cache should start empty after invalidation"
        )

        embedding = [0.9] * 128
        mock_driver = AsyncMock()
        mock_session = AsyncMock()
        mock_driver.session.return_value.__aenter__ = AsyncMock(
            return_value=mock_session,
        )
        mock_driver.session.return_value.__aexit__ = AsyncMock(
            return_value=False,
        )
        mock_session.execute_read = AsyncMock(
            return_value=[{"name": "auth-service", "id": "auth-1"}],
        )

        state = {
            "query": "unique store on miss query",
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
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._fetch_candidates",
            return_value=[{"name": "auth-service", "id": "auth-1"}],
        ), patch(
            "orchestrator.app.query_engine.run_traversal",
            return_value=[{"source": "auth", "rel": "CALLS", "target": "db"}],
        ), patch(
            "orchestrator.app.query_engine._build_traversal_acl_params",
            return_value={"is_admin": True, "acl_team": "", "acl_namespaces": []},
        ):
            result = await cypher_retrieve(state)

        assert result.get("cypher_results") is not None
        assert _SEMANTIC_CACHE.stats().size == 1, (
            "Semantic cache should contain 1 entry after a cache miss triggers "
            f"store; got size={_SEMANTIC_CACHE.stats().size}"
        )

        _SEMANTIC_CACHE.invalidate_all()
