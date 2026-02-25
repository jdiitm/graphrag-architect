from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.testclient import TestClient

from orchestrator.app.query_models import JobStatus


class TestEvaluationStore:
    def test_store_and_retrieve(self) -> None:
        from orchestrator.app.rag_evaluator import EvaluationStore

        store = EvaluationStore()
        store.put("q1", {"score": 0.9, "quality": "high"})
        result = store.get("q1")
        assert result is not None
        assert result["score"] == 0.9

    def test_missing_key_returns_none(self) -> None:
        from orchestrator.app.rag_evaluator import EvaluationStore

        store = EvaluationStore()
        assert store.get("nonexistent") is None

    def test_ttl_eviction(self) -> None:
        from orchestrator.app.rag_evaluator import EvaluationStore

        store = EvaluationStore(ttl_seconds=0.0)
        store.put("q1", {"score": 0.8})
        store._evict_expired()
        assert store.get("q1") is None


class TestEvaluateResponseFiresBackground:
    @pytest.mark.asyncio
    async def test_evaluate_returns_immediately_with_pending(self) -> None:
        from orchestrator.app.query_engine import evaluate_response

        state = {
            "query": "test query",
            "sources": [],
            "retrieval_path": "vector",
            "answer": "test answer",
        }
        with patch("orchestrator.app.query_engine.RAGEvalConfig") as mock_cfg:
            cfg = MagicMock()
            cfg.enable_evaluation = True
            cfg.low_relevance_threshold = 0.3
            mock_cfg.from_env.return_value = cfg

            with patch("orchestrator.app.query_engine._embed_query", new_callable=AsyncMock) as mock_embed:
                mock_embed.return_value = [0.1, 0.2]
                result = await evaluate_response(state)

        assert result["evaluation_score"] is None
        assert result["retrieval_quality"] == "pending"

    @pytest.mark.asyncio
    async def test_evaluate_schedules_background_task(self) -> None:
        from orchestrator.app.query_engine import evaluate_response

        state = {
            "query": "test query",
            "sources": [],
            "retrieval_path": "vector",
            "answer": "test answer",
        }
        tasks_before = len(asyncio.all_tasks())

        with patch("orchestrator.app.query_engine.RAGEvalConfig") as mock_cfg:
            cfg = MagicMock()
            cfg.enable_evaluation = True
            cfg.low_relevance_threshold = 0.3
            mock_cfg.from_env.return_value = cfg

            with patch("orchestrator.app.query_engine._embed_query", new_callable=AsyncMock) as mock_embed:
                mock_embed.return_value = [0.1, 0.2]
                with patch("orchestrator.app.query_engine._run_background_evaluation", new_callable=AsyncMock) as mock_bg:
                    result = await evaluate_response(state)

        assert result["retrieval_quality"] == "pending"


class TestEvaluationEndpoint:
    def test_evaluation_endpoint_returns_result(self) -> None:
        from orchestrator.app.main import app
        from orchestrator.app.rag_evaluator import EvaluationStore

        with patch("orchestrator.app.main._EVAL_STORE") as mock_store:
            mock_store.get.return_value = {
                "evaluation_score": 0.85,
                "retrieval_quality": "high",
                "query_id": "q123",
            }
            client = TestClient(app)
            resp = client.get("/query/q123/evaluation")
            assert resp.status_code == 200
            data = resp.json()
            assert data["evaluation_score"] == 0.85

    def test_evaluation_endpoint_returns_404_when_pending(self) -> None:
        from orchestrator.app.main import app

        with patch("orchestrator.app.main._EVAL_STORE") as mock_store:
            mock_store.get.return_value = None
            client = TestClient(app)
            resp = client.get("/query/missing-id/evaluation")
            assert resp.status_code == 404
