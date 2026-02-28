from __future__ import annotations

from typing import List
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from orchestrator.app.rag_evaluator import EvaluationResult


class TestExtractJson:
    def test_extracts_json_from_preamble_text(self) -> None:
        from orchestrator.app.rag_evaluator import _extract_json

        raw = 'Here is my analysis:\n{"faithfulness": 0.85, "groundedness": 0.9}'
        result = _extract_json(raw)
        assert result is not None
        assert result["faithfulness"] == pytest.approx(0.85)
        assert result["groundedness"] == pytest.approx(0.9)

    def test_returns_none_for_no_json_content(self) -> None:
        from orchestrator.app.rag_evaluator import _extract_json

        raw = "I cannot evaluate this content because it lacks sufficient context."
        result = _extract_json(raw)
        assert result is None

    def test_extracts_nested_json_correctly(self) -> None:
        from orchestrator.app.rag_evaluator import _extract_json

        raw = (
            "Analysis complete:\n"
            '{"faithfulness": 0.7, "details": {"confidence": 0.8}, "groundedness": 0.9}\n'
            "End of response."
        )
        result = _extract_json(raw)
        assert result is not None
        assert result["faithfulness"] == pytest.approx(0.7)
        assert result["details"]["confidence"] == pytest.approx(0.8)
        assert result["groundedness"] == pytest.approx(0.9)

    def test_returns_none_for_invalid_json_in_braces(self) -> None:
        from orchestrator.app.rag_evaluator import _extract_json

        raw = "Some text {not valid json at all} more text"
        result = _extract_json(raw)
        assert result is None


class TestJudgeOutputModel:
    def test_valid_judge_output_parses_successfully(self) -> None:
        from orchestrator.app.rag_evaluator import JudgeOutput

        output = JudgeOutput(
            faithfulness=0.8, groundedness=0.9, reasoning="Well supported",
        )
        assert output.faithfulness == pytest.approx(0.8)
        assert output.groundedness == pytest.approx(0.9)
        assert output.reasoning == "Well supported"

    def test_missing_faithfulness_raises_validation_error(self) -> None:
        from orchestrator.app.rag_evaluator import JudgeOutput

        with pytest.raises(ValidationError):
            JudgeOutput(groundedness=0.9)  # type: ignore[call-arg]

    def test_missing_groundedness_raises_validation_error(self) -> None:
        from orchestrator.app.rag_evaluator import JudgeOutput

        with pytest.raises(ValidationError):
            JudgeOutput(faithfulness=0.8)  # type: ignore[call-arg]

    def test_reasoning_is_optional(self) -> None:
        from orchestrator.app.rag_evaluator import JudgeOutput

        output = JudgeOutput(faithfulness=0.5, groundedness=0.6)
        assert output.reasoning is None

    def test_out_of_range_faithfulness_raises_validation_error(self) -> None:
        from orchestrator.app.rag_evaluator import JudgeOutput

        with pytest.raises(ValidationError):
            JudgeOutput(faithfulness=1.5, groundedness=0.9)

    def test_negative_groundedness_raises_validation_error(self) -> None:
        from orchestrator.app.rag_evaluator import JudgeOutput

        with pytest.raises(ValidationError):
            JudgeOutput(faithfulness=0.5, groundedness=-0.1)


class TestLLMEvaluatorJsonExtraction:
    @pytest.mark.asyncio
    async def test_preamble_text_extracts_valid_scores(self) -> None:
        from orchestrator.app.rag_evaluator import LLMEvaluator

        async def _judge_with_preamble(prompt: str) -> str:
            return 'Here is my analysis:\n{"faithfulness": 0.85, "groundedness": 0.9}'

        evaluator = LLMEvaluator(judge_fn=_judge_with_preamble)
        result = await evaluator.evaluate(
            query="What services depend on auth?",
            answer="payment-service depends on auth.",
            sources=[{"name": "auth-service"}, {"name": "payment-service"}],
        )
        assert result.faithfulness == pytest.approx(0.85)
        assert result.groundedness == pytest.approx(0.9)
        assert result.used_fallback is False

    @pytest.mark.asyncio
    async def test_no_json_at_all_degrades_to_fallback(self) -> None:
        from orchestrator.app.rag_evaluator import LLMEvaluator

        async def _no_json_judge(prompt: str) -> str:
            return "I apologize, I cannot evaluate this response properly."

        evaluator = LLMEvaluator(judge_fn=_no_json_judge)
        result = await evaluator.evaluate(
            query="test",
            answer="auth-service is running",
            sources=[{"name": "auth-service"}],
        )
        assert isinstance(result, EvaluationResult)
        assert 0.0 <= result.faithfulness <= 1.0
        assert 0.0 <= result.groundedness <= 1.0
        assert result.used_fallback is True

    @pytest.mark.asyncio
    async def test_pydantic_validation_rejects_out_of_range_scores(self) -> None:
        from orchestrator.app.rag_evaluator import LLMEvaluator

        async def _bad_score_judge(prompt: str) -> str:
            return '{"faithfulness": 2.5, "groundedness": -0.1}'

        evaluator = LLMEvaluator(judge_fn=_bad_score_judge)
        result = await evaluator.evaluate(
            query="test",
            answer="auth-service is running",
            sources=[{"name": "auth-service"}],
        )
        assert result.used_fallback is True


class TestEmbeddingBasedFaithfulness:
    def test_embedding_overlap_matches_similar_entities(self) -> None:
        from orchestrator.app.rag_evaluator import _compute_faithfulness

        def mock_embed(text: str) -> List[float]:
            mapping = {
                "auth-service": [1.0, 0.0, 0.0],
                "authentication-svc": [0.95, 0.05, 0.0],
            }
            return mapping.get(text, [0.33, 0.33, 0.33])

        sources = [{"name": "auth-service"}]
        answer = "authentication-svc is up"

        score, ungrounded = _compute_faithfulness(
            answer, sources, embed_fn=mock_embed,
        )
        assert score > 0.5
        assert "authentication-svc" not in ungrounded

    def test_embedding_overlap_detects_ungrounded_entities(self) -> None:
        from orchestrator.app.rag_evaluator import _compute_faithfulness

        def mock_embed(text: str) -> List[float]:
            mapping = {
                "auth-service": [1.0, 0.0, 0.0],
                "billing-engine": [0.0, 1.0, 0.0],
            }
            return mapping.get(text, [0.33, 0.33, 0.33])

        sources = [{"name": "auth-service"}]
        answer = "billing-engine processes invoices"

        score, ungrounded = _compute_faithfulness(
            answer, sources, embed_fn=mock_embed,
        )
        assert score < 1.0
        assert "billing-engine" in ungrounded

    def test_cosine_similarity_called_for_entity_comparison(self) -> None:
        from orchestrator.app.rag_evaluator import _compute_faithfulness

        embed_fn = MagicMock(side_effect=lambda text: [1.0, 0.0, 0.0])

        sources = [{"name": "auth-service"}]
        answer = "auth-service handles requests"

        with patch(
            "orchestrator.app.rag_evaluator._cosine_similarity",
            return_value=0.99,
        ) as mock_cosine:
            _compute_faithfulness(answer, sources, embed_fn=embed_fn)
            mock_cosine.assert_called()
            embed_fn.assert_called()

    def test_without_embed_fn_falls_back_to_lexical(self) -> None:
        from orchestrator.app.rag_evaluator import _compute_faithfulness

        sources = [{"name": "auth-service"}]
        answer = "auth-service is running"

        score, ungrounded = _compute_faithfulness(answer, sources)
        assert score >= 0.5
