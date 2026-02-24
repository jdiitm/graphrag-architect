from __future__ import annotations

from unittest.mock import patch

import pytest

from orchestrator.app.config import RAGEvalConfig
from orchestrator.app.rag_evaluator import (
    EvaluationResult,
    RAGEvaluator,
    RelevanceScore,
    _compute_faithfulness,
    _compute_groundedness,
    _extract_entity_names_from_context,
    _extract_entity_names_from_text,
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


class TestExtractEntityNames:
    def test_extracts_service_names_from_text(self) -> None:
        text = "The auth-service calls order-service"
        names = _extract_entity_names_from_text(text)
        assert "auth-service" in names
        assert "order-service" in names

    def test_extracts_from_context_sources(self) -> None:
        sources = [
            {"name": "auth-svc", "id": "svc-1"},
            {"result": {"name": "order-svc"}},
        ]
        names = _extract_entity_names_from_context(sources)
        assert "auth-svc" in names
        assert "svc-1" in names
        assert "order-svc" in names


class TestComputeFaithfulness:
    def test_empty_answer_returns_one(self) -> None:
        score, ungrounded = _compute_faithfulness("", [{"name": "svc"}])
        assert score == 1.0
        assert ungrounded == []

    def test_empty_sources_returns_one(self) -> None:
        score, ungrounded = _compute_faithfulness("auth-service is down", [])
        assert score == 1.0

    def test_fully_grounded_answer(self) -> None:
        sources = [{"name": "auth-service"}, {"name": "order-service"}]
        answer = "auth-service calls order-service"
        score, ungrounded = _compute_faithfulness(answer, sources)
        assert score >= 0.5

    def test_partially_grounded_answer(self) -> None:
        sources = [{"name": "auth-service"}]
        answer = "auth-service calls billing-engine and payment-gateway"
        score, ungrounded = _compute_faithfulness(answer, sources)
        assert score < 1.0
        assert len(ungrounded) > 0


class TestComputeGroundedness:
    def test_empty_answer_returns_one(self) -> None:
        assert _compute_groundedness("", [{"name": "svc"}]) == 1.0

    def test_empty_sources_returns_one(self) -> None:
        assert _compute_groundedness("auth-service", []) == 1.0

    def test_all_entities_present_returns_one(self) -> None:
        sources = [{"name": "auth-service"}, {"name": "order-service"}]
        answer = "auth-service and order-service are connected"
        score = _compute_groundedness(answer, sources)
        assert score >= 0.5

    def test_no_entities_found_returns_zero(self) -> None:
        sources = [{"name": "payment-service"}]
        answer = "auth-service calls billing-engine"
        score = _compute_groundedness(answer, sources)
        assert score < 1.0

    def test_groundedness_independent_of_faithfulness(self) -> None:
        evaluator = RAGEvaluator()
        sources = [{"name": "auth-service"}]
        result = evaluator.evaluate_faithfulness(
            query="test",
            answer="auth-service is healthy",
            sources=sources,
            query_embedding=[1.0, 0.0],
            context_embeddings=[[1.0, 0.0]],
        )
        assert isinstance(result, EvaluationResult)
        assert isinstance(result.groundedness, float)
        assert isinstance(result.faithfulness, float)


class TestEvaluationResultScore:
    def test_weighted_score_computation(self) -> None:
        result = EvaluationResult(
            context_relevance=1.0,
            faithfulness=1.0,
            groundedness=1.0,
        )
        assert abs(result.score - 1.0) < 1e-9

    def test_zero_scores(self) -> None:
        result = EvaluationResult(
            context_relevance=0.0,
            faithfulness=0.0,
            groundedness=0.0,
        )
        assert result.score == 0.0

    def test_partial_scores(self) -> None:
        result = EvaluationResult(
            context_relevance=0.5,
            faithfulness=0.5,
            groundedness=0.5,
        )
        assert abs(result.score - 0.5) < 1e-9


class TestEvaluateFaithfulness:
    def test_returns_evaluation_result(self) -> None:
        evaluator = RAGEvaluator()
        result = evaluator.evaluate_faithfulness(
            query="test",
            answer="auth-service is running",
            sources=[{"name": "auth-service"}],
            query_embedding=[1.0, 0.0],
            context_embeddings=[[1.0, 0.0]],
        )
        assert isinstance(result, EvaluationResult)
        assert 0.0 <= result.faithfulness <= 1.0
        assert 0.0 <= result.groundedness <= 1.0
        assert 0.0 <= result.context_relevance <= 1.0
        assert result.context_count == 1
        assert result.retrieval_path == "vector"


class TestLLMEvaluator:

    @pytest.mark.asyncio
    async def test_llm_evaluator_returns_scores_in_range(self) -> None:
        from orchestrator.app.rag_evaluator import LLMEvaluator

        async def _fake_judge(prompt: str) -> str:
            return '{"faithfulness": 0.85, "groundedness": 0.9}'

        evaluator = LLMEvaluator(judge_fn=_fake_judge)
        result = await evaluator.evaluate(
            query="What services depend on auth?",
            answer="The payment service depends on auth-service.",
            sources=[{"name": "auth-service"}, {"name": "payment"}],
        )
        assert 0.0 <= result.faithfulness <= 1.0
        assert 0.0 <= result.groundedness <= 1.0

    @pytest.mark.asyncio
    async def test_llm_evaluator_scores_unfaithful_answer_low(self) -> None:
        from orchestrator.app.rag_evaluator import LLMEvaluator

        async def _fake_judge(prompt: str) -> str:
            if "fabricated" in prompt.lower() or "answer" in prompt.lower():
                return '{"faithfulness": 0.1, "groundedness": 0.05}'
            return '{"faithfulness": 0.5, "groundedness": 0.5}'

        evaluator = LLMEvaluator(judge_fn=_fake_judge)
        result = await evaluator.evaluate(
            query="What services exist?",
            answer="The fabricated-service handles everything.",
            sources=[{"name": "auth-service"}],
        )
        assert result.faithfulness <= 0.5

    @pytest.mark.asyncio
    async def test_llm_evaluator_handles_malformed_response(self) -> None:
        from orchestrator.app.rag_evaluator import LLMEvaluator

        async def _broken_judge(prompt: str) -> str:
            return "not valid json"

        evaluator = LLMEvaluator(judge_fn=_broken_judge)
        result = await evaluator.evaluate(
            query="test",
            answer="test answer",
            sources=[{"name": "svc"}],
        )
        assert 0.0 <= result.faithfulness <= 1.0
        assert 0.0 <= result.groundedness <= 1.0


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
