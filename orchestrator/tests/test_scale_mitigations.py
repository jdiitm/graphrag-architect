from __future__ import annotations

import asyncio
import concurrent.futures
import json
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.agentic_traversal import (
    _SAMPLED_NEIGHBOR_TEMPLATE,
    _NEIGHBOR_DISCOVERY_TEMPLATE,
    _BATCHED_NEIGHBOR_TEMPLATE,
    _BOUNDED_PATH_TEMPLATE,
    execute_hop,
)
from orchestrator.app.semantic_cache import (
    CacheConfig,
    RedisSemanticQueryCache,
    _embedding_hash,
)
from orchestrator.app.vector_sync_outbox import (
    RedisOutboxStore,
    VectorSyncEvent,
)


class TestSemanticCacheNoScan:

    @pytest.mark.asyncio
    async def test_redis_lookup_uses_direct_get_not_scan(self) -> None:
        mock_redis = AsyncMock()
        embedding = [0.1] * 32
        key_hash = _embedding_hash(embedding) + "|"
        stored_payload = json.dumps({
            "query": "test query",
            "embedding": embedding[:32],
            "result": {"answer": "cached"},
            "tenant_id": "t1",
            "acl_key": "",
        })
        mock_redis.get = AsyncMock(return_value=stored_payload)
        mock_redis.scan = AsyncMock(side_effect=AssertionError(
            "SCAN must not be called on the hot lookup path"
        ))

        with patch(
            "orchestrator.app.semantic_cache.create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379",
                config=CacheConfig(similarity_threshold=0.9),
            )
            result = await cache._redis_lookup(embedding, tenant_id="t1")

        assert result is not None
        assert result == {"answer": "cached"}
        mock_redis.get.assert_called_once()
        called_key = mock_redis.get.call_args[0][0]
        assert cache._prefix in called_key

    @pytest.mark.asyncio
    async def test_redis_lookup_returns_none_on_cache_miss(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value=None)

        with patch(
            "orchestrator.app.semantic_cache.create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379",
                config=CacheConfig(similarity_threshold=0.9),
            )
            result = await cache._redis_lookup([0.5] * 32, tenant_id="t1")

        assert result is None

    @pytest.mark.asyncio
    async def test_redis_lookup_enforces_tenant_isolation(self) -> None:
        mock_redis = AsyncMock()
        stored_payload = json.dumps({
            "query": "test",
            "embedding": [0.1] * 32,
            "result": {"answer": "wrong tenant"},
            "tenant_id": "other_tenant",
            "acl_key": "",
        })
        mock_redis.get = AsyncMock(return_value=stored_payload)

        with patch(
            "orchestrator.app.semantic_cache.create_async_redis",
            return_value=mock_redis,
        ), patch(
            "orchestrator.app.semantic_cache.require_redis",
        ):
            cache = RedisSemanticQueryCache(
                redis_url="redis://fake:6379",
                config=CacheConfig(similarity_threshold=0.9),
            )
            result = await cache._redis_lookup(
                [0.1] * 32, tenant_id="my_tenant",
            )

        assert result is None


class TestAtomicOutboxClaims:

    def test_redis_outbox_store_has_claim_lua_script(self) -> None:
        assert hasattr(RedisOutboxStore, "_CLAIM_LUA_SCRIPT"), (
            "RedisOutboxStore must define a _CLAIM_LUA_SCRIPT for atomic claiming"
        )
        script = RedisOutboxStore._CLAIM_LUA_SCRIPT
        assert isinstance(script, str)
        assert len(script) > 0

    @pytest.mark.asyncio
    async def test_claim_pending_uses_eval_for_atomicity(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.eval = AsyncMock(return_value=[])
        store = RedisOutboxStore(redis_conn=mock_redis)

        await store.claim_pending("worker-1", limit=5, lease_seconds=60.0)

        mock_redis.eval.assert_called_once()
        first_arg = mock_redis.eval.call_args[0][0]
        assert first_arg == RedisOutboxStore._CLAIM_LUA_SCRIPT


class TestProcessPoolASTExtraction:

    def test_graph_builder_exposes_process_pool_accessor(self) -> None:
        from orchestrator.app import graph_builder
        assert hasattr(graph_builder, "_get_process_pool"), (
            "graph_builder must expose _get_process_pool for CPU-bound work"
        )

    def test_get_process_pool_returns_process_pool_executor(self) -> None:
        from orchestrator.app.graph_builder import _get_process_pool
        pool = _get_process_pool()
        assert isinstance(pool, concurrent.futures.ProcessPoolExecutor)

    @pytest.mark.asyncio
    async def test_parse_source_ast_uses_process_pool_not_thread(self) -> None:
        from orchestrator.app.graph_builder import parse_source_ast

        mock_pool = MagicMock(spec=concurrent.futures.ProcessPoolExecutor)
        mock_future = asyncio.Future()
        mock_future.set_result((MagicMock(services=[], calls=[]),
                                MagicMock(services=[], calls=[])))

        mock_loop = MagicMock()
        mock_loop.run_in_executor = AsyncMock(return_value=mock_future.result())

        state = {
            "raw_files": [{"path": "svc.go", "content": "package main"}],
            "extraction_checkpoint": {},
            "tenant_id": "default",
        }

        with patch(
            "orchestrator.app.graph_builder._get_process_pool",
            return_value=mock_pool,
        ), patch(
            "asyncio.get_running_loop",
            return_value=mock_loop,
        ), patch(
            "orchestrator.app.graph_builder.get_tracer",
        ) as mock_tracer:
            mock_span = MagicMock()
            mock_span.__enter__ = MagicMock(return_value=mock_span)
            mock_span.__exit__ = MagicMock(return_value=False)
            mock_tracer.return_value.start_as_current_span.return_value = mock_span

            await parse_source_ast(state)

        mock_loop.run_in_executor.assert_called_once()
        executor_arg = mock_loop.run_in_executor.call_args[0][0]
        assert executor_arg is mock_pool


class TestDeterministicSampling:

    def test_sampled_neighbor_template_has_no_rand(self) -> None:
        assert "rand()" not in _SAMPLED_NEIGHBOR_TEMPLATE, (
            "rand() in ORDER BY forces full neighborhood materialization "
            "and causes Neo4j OOM on supernodes. Use a deterministic "
            "tie-breaker instead."
        )

    def test_sampled_neighbor_template_has_deterministic_tiebreaker(self) -> None:
        assert "target.id" in _SAMPLED_NEIGHBOR_TEMPLATE, (
            "Sampled neighbor template needs a deterministic tie-breaker "
            "(target.id) for stable, indexable sorting."
        )

    def test_sampled_template_still_prioritizes_pagerank(self) -> None:
        assert "pagerank" in _SAMPLED_NEIGHBOR_TEMPLATE
        assert "degree" in _SAMPLED_NEIGHBOR_TEMPLATE


class TestTraversalACLBypass:

    def test_execute_hop_accepts_skip_acl_parameter(self) -> None:
        import inspect
        sig = inspect.signature(execute_hop)
        assert "skip_acl" in sig.parameters, (
            "execute_hop must accept skip_acl parameter for physical "
            "isolation optimization"
        )

    @pytest.mark.asyncio
    async def test_execute_hop_omits_acl_predicates_when_skip_acl_true(
        self,
    ) -> None:
        captured_queries: List[str] = []

        async def _capture_tx(tx_func, **kwargs):
            mock_tx = AsyncMock()
            mock_result = AsyncMock()
            mock_result.data = AsyncMock(return_value=[])

            async def _capture_run(query, **params):
                captured_queries.append(query)
                return mock_result

            mock_tx.run = _capture_run
            return await tx_func(mock_tx)

        mock_session = AsyncMock()
        mock_session.execute_read = _capture_tx
        mock_driver = MagicMock()

        async def _session_ctx(**kwargs):
            return mock_session

        class _SessionCtx:
            async def __aenter__(self):
                return mock_session
            async def __aexit__(self, *args):
                pass

        mock_driver.session = lambda **kw: _SessionCtx()

        await execute_hop(
            driver=mock_driver,
            source_id="svc-1",
            tenant_id="tenant-1",
            acl_params={"is_admin": False, "acl_team": "eng",
                        "acl_namespaces": ["ns1"]},
            skip_acl=True,
        )

        assert len(captured_queries) >= 1
        for query in captured_queries:
            assert "namespace_acl" not in query, (
                f"ACL predicate found in query when skip_acl=True: {query}"
            )

    @pytest.mark.asyncio
    async def test_execute_hop_includes_acl_when_skip_acl_false(self) -> None:
        captured_queries: List[str] = []

        async def _capture_tx(tx_func, **kwargs):
            mock_tx = AsyncMock()
            mock_result = AsyncMock()
            mock_result.data = AsyncMock(return_value=[
                {"degree": 10},
            ])

            async def _capture_run(query, **params):
                captured_queries.append(query)
                return mock_result

            mock_tx.run = _capture_run
            return await tx_func(mock_tx)

        mock_session = AsyncMock()
        mock_session.execute_read = _capture_tx

        class _SessionCtx:
            async def __aenter__(self):
                return mock_session
            async def __aexit__(self, *args):
                pass

        mock_driver = MagicMock()
        mock_driver.session = lambda **kw: _SessionCtx()

        await execute_hop(
            driver=mock_driver,
            source_id="svc-1",
            tenant_id="tenant-1",
            acl_params={"is_admin": False, "acl_team": "eng",
                        "acl_namespaces": ["ns1"]},
            skip_acl=False,
        )

        neighbor_queries = [q for q in captured_queries if "target" in q]
        assert len(neighbor_queries) >= 1
        for query in neighbor_queries:
            assert "namespace_acl" in query, (
                f"ACL predicate missing when skip_acl=False: {query}"
            )
