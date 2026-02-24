from __future__ import annotations

from unittest.mock import patch

import pytest

from orchestrator.app.config import RAGEvalConfig
from orchestrator.app.rag_evaluator import (
    RAGEvaluator,
    RelevanceScore,
    evaluate_relevance,
)


class TestEvaluateRelevance:
    def test_identical_vectors_returns_one(self) -> None:
        vec = [1.0, 2.0, 3.0]
        result = evaluate_relevance(vec, [vec])
        assert abs(result - 1.0) < 1e-9

    def test_orthogonal_vectors_returns_zero(self) -> None:
        query = [1.0, 0.0, 0.0]
        contexts = [[0.0, 1.0, 0.0]]
        result = evaluate_relevance(query, contexts)
        assert abs(result) < 1e-9

    def test_empty_context_returns_zero(self) -> None:
        result = evaluate_relevance([1.0, 2.0, 3.0], [])
        assert result == 0.0

    def test_multiple_contexts_averages_similarity(self) -> None:
        query = [1.0, 0.0, 0.0]
        contexts = [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
        ]
        result = evaluate_relevance(query, contexts)
        assert abs(result - 0.5) < 1e-9


class TestRAGEvaluator:
    def test_evaluate_returns_relevance_score_with_correct_fields(self) -> None:
        evaluator = RAGEvaluator(low_relevance_threshold=0.3)
        query = "test query"
        query_emb = [1.0, 0.0, 0.0]
        context_embs = [[1.0, 0.0, 0.0], [0.5, 0.5, 0.0]]
        score = evaluator.evaluate(
            query, query_emb, context_embs, retrieval_path="vector"
        )
        assert isinstance(score, RelevanceScore)
        assert score.query == query
        assert 0.0 <= score.score <= 1.0
        assert score.context_count == 2
        assert score.retrieval_path == "vector"

    def test_is_low_relevance_returns_true_when_below_threshold(self) -> None:
        evaluator = RAGEvaluator(low_relevance_threshold=0.5)
        score = RelevanceScore(
            query="q", score=0.2, context_count=3, retrieval_path="vector"
        )
        assert evaluator.is_low_relevance(score) is True

    def test_is_low_relevance_returns_false_when_at_or_above_threshold(
        self,
    ) -> None:
        evaluator = RAGEvaluator(low_relevance_threshold=0.5)
        score_low = RelevanceScore(
            query="q", score=0.5, context_count=3, retrieval_path="vector"
        )
        score_high = RelevanceScore(
            query="q", score=0.8, context_count=3, retrieval_path="vector"
        )
        assert evaluator.is_low_relevance(score_low) is False
        assert evaluator.is_low_relevance(score_high) is False


class TestRAGEvalConfig:
    def test_from_env_defaults(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            cfg = RAGEvalConfig.from_env()
            assert cfg.low_relevance_threshold == 0.3
            assert cfg.enable_evaluation is True

    def test_from_env_reads_environment_variables(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "RAG_LOW_RELEVANCE_THRESHOLD": "0.6",
                "RAG_ENABLE_EVALUATION": "false",
            },
            clear=True,
        ):
            cfg = RAGEvalConfig.from_env()
            assert cfg.low_relevance_threshold == 0.6
            assert cfg.enable_evaluation is False
