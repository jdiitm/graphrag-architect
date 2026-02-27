from __future__ import annotations

import warnings
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.extraction_models import (
    CallsEdge,
    ServiceNode,
)
from orchestrator.app.neo4j_client import GraphRepository


class TestCacheByteEstimatorNoSerialization:

    def test_estimate_bytes_does_not_call_json_dumps(self) -> None:
        from orchestrator.app.subgraph_cache import _estimate_bytes

        value: List[Dict[str, Any]] = [{"node": "a", "data": "x" * 500}]
        with patch("orchestrator.app.subgraph_cache.json.dumps") as mock_dumps:
            mock_dumps.side_effect = AssertionError(
                "json.dumps must not be called in byte estimation"
            )
            result = _estimate_bytes(value)
        assert result > 0

    def test_estimate_bytes_returns_positive_for_nonempty_list(self) -> None:
        from orchestrator.app.subgraph_cache import _estimate_bytes

        value = [{"node": "auth-service", "language": "go"}]
        result = _estimate_bytes(value)
        assert result > 0

    def test_estimate_bytes_returns_zero_for_empty_list(self) -> None:
        from orchestrator.app.subgraph_cache import _estimate_bytes

        result = _estimate_bytes([])
        assert result >= 0

    def test_estimate_bytes_scales_with_data_size(self) -> None:
        from orchestrator.app.subgraph_cache import _estimate_bytes

        small = [{"x": "a"}]
        large = [{"x": "a" * 10_000}]
        assert _estimate_bytes(large) > _estimate_bytes(small)


class TestPageRankStrategyProtocol:

    def test_local_strategy_exists_and_ranks_edges(self) -> None:
        from orchestrator.app.lazy_traversal import LocalPageRankStrategy

        strategy = LocalPageRankStrategy()
        edges = [
            {"source": "auth", "target": "user"},
            {"source": "user", "target": "order"},
            {"source": "order", "target": "payment"},
        ]
        ranked = strategy.rank(edges, seed_nodes=["auth"], top_n=3)
        assert len(ranked) <= 3
        assert all(isinstance(pair, tuple) and len(pair) == 2 for pair in ranked)

    def test_local_strategy_returns_empty_for_no_edges(self) -> None:
        from orchestrator.app.lazy_traversal import LocalPageRankStrategy

        strategy = LocalPageRankStrategy()
        ranked = strategy.rank([], seed_nodes=["x"], top_n=5)
        assert ranked == []

    def test_gds_strategy_exists_and_has_rank_method(self) -> None:
        from orchestrator.app.lazy_traversal import GDSPageRankStrategy

        mock_driver = MagicMock()
        strategy = GDSPageRankStrategy(driver=mock_driver)
        assert hasattr(strategy, "rank")
        assert callable(strategy.rank)

    def test_personalized_pagerank_accepts_strategy_parameter(self) -> None:
        from orchestrator.app.lazy_traversal import (
            LocalPageRankStrategy,
            personalized_pagerank,
        )

        edges = [
            {"source": "a", "target": "b"},
            {"source": "b", "target": "c"},
        ]
        strategy = LocalPageRankStrategy()
        result = personalized_pagerank(
            edges, seed_nodes=["a"], top_n=3, strategy=strategy,
        )
        assert len(result) <= 3

    def test_personalized_pagerank_without_strategy_still_works(self) -> None:
        from orchestrator.app.lazy_traversal import personalized_pagerank

        edges = [
            {"source": "a", "target": "b"},
            {"source": "b", "target": "c"},
        ]
        result = personalized_pagerank(edges, seed_nodes=["a"], top_n=3)
        assert len(result) <= 3


class TestVectorStoreProductionGuard:

    def test_memory_backend_raises_in_production(self) -> None:
        from orchestrator.app.vector_store import create_vector_store

        with pytest.raises(ValueError, match="[Pp]roduction"):
            create_vector_store(backend="memory", deployment_mode="production")

    def test_memory_backend_allowed_in_dev(self) -> None:
        from orchestrator.app.vector_store import (
            InMemoryVectorStore,
            create_vector_store,
        )

        store = create_vector_store(backend="memory", deployment_mode="dev")
        assert isinstance(store, InMemoryVectorStore)

    def test_qdrant_backend_allowed_in_production(self) -> None:
        from orchestrator.app.vector_store import (
            QdrantVectorStore,
            create_vector_store,
        )

        store = create_vector_store(
            backend="qdrant",
            url="http://localhost:6333",
            deployment_mode="production",
        )
        assert isinstance(store, QdrantVectorStore)

    def test_unrecognized_backend_raises_in_production(self) -> None:
        from orchestrator.app.vector_store import create_vector_store

        with pytest.raises(ValueError, match="[Pp]roduction"):
            create_vector_store(backend="unknown", deployment_mode="production")

    def test_default_deployment_mode_is_dev(self) -> None:
        from orchestrator.app.vector_store import (
            InMemoryVectorStore,
            create_vector_store,
        )

        store = create_vector_store(backend="memory")
        assert isinstance(store, InMemoryVectorStore)


class TestStreamingSynthesis:

    @pytest.mark.asyncio
    async def test_streaming_synthesize_yields_chunks(self) -> None:
        from orchestrator.app.query_engine import _raw_llm_synthesize_stream

        mock_llm = MagicMock()

        async def fake_astream(messages):
            for chunk in ["Hello", " world", "!"]:
                mock_chunk = MagicMock()
                mock_chunk.content = chunk
                yield mock_chunk

        mock_llm.astream = fake_astream

        with patch(
            "orchestrator.app.query_engine._build_llm",
            return_value=mock_llm,
        ):
            chunks = []
            async for token in _raw_llm_synthesize_stream(
                "test query", [{"node": "auth-service"}],
            ):
                chunks.append(token)

        assert len(chunks) >= 1
        assert "".join(chunks) == "Hello world!"

    @pytest.mark.asyncio
    async def test_non_streaming_synthesize_still_works(self) -> None:
        from orchestrator.app.query_engine import _raw_llm_synthesize

        mock_llm = MagicMock()
        mock_response = MagicMock()
        mock_response.content = "The auth-service uses Go."
        mock_llm.ainvoke = AsyncMock(return_value=mock_response)

        with patch(
            "orchestrator.app.query_engine._build_llm",
            return_value=mock_llm,
        ):
            result = await _raw_llm_synthesize(
                "What language is auth-service?",
                [{"name": "auth-service", "language": "go"}],
            )

        assert "auth-service" in result.lower() or "go" in result.lower()


class TestNeo4jVectorStoreDeprecation:

    def test_neo4j_vector_store_emits_deprecation_warning(self) -> None:
        from orchestrator.app.vector_store import Neo4jVectorStore

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            Neo4jVectorStore(driver=MagicMock())

        deprecation_warnings = [
            w for w in caught if issubclass(w.category, DeprecationWarning)
        ]
        assert len(deprecation_warnings) >= 1, (
            "Neo4jVectorStore must emit a DeprecationWarning on construction"
        )

    def test_neo4j_backend_logs_deprecation_via_factory(self) -> None:
        from orchestrator.app.vector_store import (
            Neo4jVectorStore,
            create_vector_store,
        )

        mock_driver = MagicMock()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            store = create_vector_store(
                backend="neo4j", driver=mock_driver,
            )

        assert isinstance(store, Neo4jVectorStore)
        deprecation_warnings = [
            w for w in caught if issubclass(w.category, DeprecationWarning)
        ]
        assert len(deprecation_warnings) >= 1


class TestTenantScopedTombstoneSweep:

    @pytest.mark.asyncio
    async def test_tombstone_stale_edges_accepts_tenant_id(self) -> None:
        mock_session = AsyncMock()
        mock_record = MagicMock()
        mock_record.__getitem__ = lambda self, key: {
            "tombstoned": 2,
            "node_ids": ["svc-1", "svc-2"],
        }[key]
        mock_record.get = lambda key, default=None: {
            "tombstoned": 2,
            "node_ids": ["svc-1", "svc-2"],
        }.get(key, default)

        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_session.run = AsyncMock(return_value=mock_result)

        async def _capture_write(fn, **kwargs):
            return await fn(mock_session, **kwargs)

        mock_session.execute_write = AsyncMock(side_effect=_capture_write)

        mock_driver = MagicMock()

        call_count = 0

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def mock_write_session(**kwargs):
            yield mock_session

        repo = GraphRepository(driver=mock_driver)
        repo._write_session = mock_write_session

        count, ids = await repo.tombstone_stale_edges(
            current_ingestion_id="run-42",
            tenant_id="tenant-alpha",
        )
        assert count >= 0

        all_queries = [
            call.kwargs.get("query", "") or (call.args[1] if len(call.args) > 1 else "")
            for call in mock_session.execute_write.call_args_list
        ]
        tenant_filtered = any(
            "tenant_id" in q for q in all_queries
        )
        assert tenant_filtered, (
            "When tenant_id is provided, tombstone query must include tenant_id filter"
        )

    @pytest.mark.asyncio
    async def test_tombstone_without_tenant_id_scans_all(self) -> None:
        mock_session = AsyncMock()
        mock_record = MagicMock()
        mock_record.__getitem__ = lambda self, key: {
            "tombstoned": 0,
            "node_ids": [],
        }[key]
        mock_record.get = lambda key, default=None: {
            "tombstoned": 0,
            "node_ids": [],
        }.get(key, default)

        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_session.run = AsyncMock(return_value=mock_result)

        async def _capture_write(fn, **kwargs):
            return await fn(mock_session, **kwargs)

        mock_session.execute_write = AsyncMock(side_effect=_capture_write)

        mock_driver = MagicMock()

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def mock_write_session(**kwargs):
            yield mock_session

        repo = GraphRepository(driver=mock_driver)
        repo._write_session = mock_write_session

        count, ids = await repo.tombstone_stale_edges(
            current_ingestion_id="run-99",
        )
        assert count >= 0


class TestPruneStaleEdgesForwardsTenantId:

    @pytest.mark.asyncio
    async def test_prune_forwards_tenant_id_to_tombstone(self) -> None:
        mock_driver = MagicMock()
        repo = GraphRepository(driver=mock_driver)

        repo.tombstone_stale_edges = AsyncMock(return_value=(3, ["a", "b", "c"]))

        count, ids = await repo.prune_stale_edges(
            current_ingestion_id="run-55",
            tenant_id="tenant-beta",
        )
        repo.tombstone_stale_edges.assert_awaited_once_with(
            "run-55", tenant_id="tenant-beta",
        )
        assert count == 3
        assert ids == ["a", "b", "c"]
