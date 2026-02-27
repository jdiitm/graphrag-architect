from __future__ import annotations

import logging
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_COMMON_ENV = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


class TestIngestionConfig:

    def test_default_sink_batch_size(self) -> None:
        from orchestrator.app.config import IngestionConfig
        cfg = IngestionConfig()
        assert cfg.sink_batch_size == 500

    @patch.dict("os.environ", {"SINK_BATCH_SIZE": "1000"})
    def test_from_env_reads_sink_batch_size(self) -> None:
        from orchestrator.app.config import IngestionConfig
        cfg = IngestionConfig.from_env()
        assert cfg.sink_batch_size == 1000

    @patch.dict("os.environ", {"SINK_BATCH_SIZE": "50"})
    def test_from_env_clamps_below_minimum(self) -> None:
        from orchestrator.app.config import IngestionConfig
        cfg = IngestionConfig.from_env()
        assert cfg.sink_batch_size == 100

    @patch.dict("os.environ", {"SINK_BATCH_SIZE": "10000"})
    def test_from_env_clamps_above_maximum(self) -> None:
        from orchestrator.app.config import IngestionConfig
        cfg = IngestionConfig.from_env()
        assert cfg.sink_batch_size == 5000

    @patch.dict("os.environ", {}, clear=False)
    def test_from_env_uses_default_when_unset(self) -> None:
        import os
        os.environ.pop("SINK_BATCH_SIZE", None)
        from orchestrator.app.config import IngestionConfig
        cfg = IngestionConfig.from_env()
        assert cfg.sink_batch_size == 500


class TestGlobalCacheInvalidationRejected:

    @pytest.mark.asyncio
    async def test_empty_tenant_id_raises(self) -> None:
        from orchestrator.app.graph_builder import (
            IngestRejectionError,
            invalidate_caches_after_ingest,
        )

        with pytest.raises(IngestRejectionError):
            await invalidate_caches_after_ingest(tenant_id="")

    @pytest.mark.asyncio
    async def test_valid_tenant_id_does_not_raise(self) -> None:
        from orchestrator.app.graph_builder import invalidate_caches_after_ingest

        mock_cache = MagicMock()
        mock_cache.invalidate_tenant = MagicMock()
        with (
            patch(
                "orchestrator.app.query_engine._SUBGRAPH_CACHE",
                mock_cache,
            ),
            patch(
                "orchestrator.app.query_engine._SEMANTIC_CACHE",
                None,
            ),
        ):
            await invalidate_caches_after_ingest(tenant_id="tenant-abc")

        mock_cache.invalidate_tenant.assert_called_once_with("tenant-abc")


class TestDurableOutboxWiring:

    @pytest.mark.asyncio
    async def test_drain_uses_durable_drainer_when_redis_configured(self) -> None:
        from orchestrator.app.graph_builder import create_outbox_drainer
        from orchestrator.app.vector_sync_outbox import DurableOutboxDrainer

        mock_redis = AsyncMock()
        mock_vs = AsyncMock()
        drainer = create_outbox_drainer(
            redis_conn=mock_redis, vector_store=mock_vs,
        )
        assert isinstance(drainer, DurableOutboxDrainer)

    @pytest.mark.asyncio
    async def test_drain_uses_inmemory_drainer_when_no_redis(self) -> None:
        from orchestrator.app.graph_builder import create_outbox_drainer
        from orchestrator.app.vector_sync_outbox import OutboxDrainer

        mock_vs = AsyncMock()
        drainer = create_outbox_drainer(
            redis_conn=None, vector_store=mock_vs,
        )
        assert isinstance(drainer, OutboxDrainer)


class TestEmbeddingDegradationNotice:

    @pytest.mark.asyncio
    async def test_synthesis_includes_degradation_notice_when_flagged(self) -> None:
        from orchestrator.app.query_engine import _do_synthesize

        captured_context: List[List[Dict[str, Any]]] = []

        async def _spy_synthesize(
            query: str, context: List[Dict[str, Any]], **kwargs: Any,
        ) -> str:
            captured_context.append(context)
            return "synthesized answer"

        state = {
            "query": "What services call the auth service?",
            "candidates": [{"name": "auth", "type": "Service"}],
            "cypher_results": [],
            "retrieval_degraded": True,
        }

        with patch(
            "orchestrator.app.query_engine._llm_synthesize",
            side_effect=_spy_synthesize,
        ):
            result = await _do_synthesize(state)

        assert len(captured_context) == 1
        context_items = captured_context[0]
        degradation_items = [
            c for c in context_items
            if isinstance(c, dict) and c.get("_degradation_notice")
        ]
        assert len(degradation_items) == 1, (
            "Expected exactly one degradation notice in context when "
            "retrieval_degraded is True"
        )

    @pytest.mark.asyncio
    async def test_synthesis_no_notice_when_not_degraded(self) -> None:
        from orchestrator.app.query_engine import _do_synthesize

        captured_context: List[List[Dict[str, Any]]] = []

        async def _spy_synthesize(
            query: str, context: List[Dict[str, Any]], **kwargs: Any,
        ) -> str:
            captured_context.append(context)
            return "synthesized answer"

        state = {
            "query": "What services call the auth service?",
            "candidates": [{"name": "auth", "type": "Service"}],
            "cypher_results": [],
        }

        with patch(
            "orchestrator.app.query_engine._llm_synthesize",
            side_effect=_spy_synthesize,
        ):
            result = await _do_synthesize(state)

        assert len(captured_context) == 1
        context_items = captured_context[0]
        degradation_items = [
            c for c in context_items
            if isinstance(c, dict) and c.get("_degradation_notice")
        ]
        assert len(degradation_items) == 0, (
            "Expected no degradation notice when retrieval_degraded is not set"
        )


class TestRetrievalDegradedStateField:

    def test_query_state_accepts_retrieval_degraded(self) -> None:
        from orchestrator.app.query_models import QueryState
        state: QueryState = {
            "query": "test",
            "max_results": 10,
            "complexity": "entity_lookup",
            "retrieval_path": "vector",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
            "authorization": "",
            "evaluation_score": None,
            "retrieval_quality": "skipped",
            "query_id": "",
            "retrieval_degraded": True,
        }
        assert state["retrieval_degraded"] is True
