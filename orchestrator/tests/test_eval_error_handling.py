from __future__ import annotations

from typing import Any, Dict, Optional

import pytest

from orchestrator.app.rag_evaluator import EvaluationStore


class TestEvaluationStoreErrorFiltering:

    def test_error_entries_excluded_from_valid_scores(self) -> None:
        store = EvaluationStore()
        store.put("q1", {"evaluation_score": 0.85, "retrieval_quality": "good", "query_id": "q1"})
        store.put("q2", {"evaluation_score": None, "retrieval_quality": "error", "query_id": "q2"})
        store.put("q3", {"evaluation_score": 0.72, "retrieval_quality": "good", "query_id": "q3"})

        valid = store.get_valid_scores()
        assert len(valid) == 2
        scores = [v["evaluation_score"] for v in valid]
        assert None not in scores
        assert all(isinstance(s, float) for s in scores)

    def test_no_valid_scores_returns_empty(self) -> None:
        store = EvaluationStore()
        store.put("q1", {"evaluation_score": None, "retrieval_quality": "error", "query_id": "q1"})
        valid = store.get_valid_scores()
        assert valid == []

    def test_skipped_evaluations_excluded(self) -> None:
        store = EvaluationStore()
        store.put("q1", {"evaluation_score": None, "retrieval_quality": "skipped", "query_id": "q1"})
        store.put("q2", {"evaluation_score": 0.9, "retrieval_quality": "good", "query_id": "q2"})
        valid = store.get_valid_scores()
        assert len(valid) == 1

    def test_pending_evaluations_excluded(self) -> None:
        store = EvaluationStore()
        store.put("q1", {"evaluation_score": None, "retrieval_quality": "pending", "query_id": "q1"})
        valid = store.get_valid_scores()
        assert valid == []

    def test_aggregate_score_ignores_errors(self) -> None:
        store = EvaluationStore()
        store.put("q1", {"evaluation_score": 0.8, "retrieval_quality": "good", "query_id": "q1"})
        store.put("q2", {"evaluation_score": None, "retrieval_quality": "error", "query_id": "q2"})
        store.put("q3", {"evaluation_score": 0.6, "retrieval_quality": "good", "query_id": "q3"})

        avg = store.average_score()
        assert avg == pytest.approx(0.7)

    def test_aggregate_score_with_no_valid_entries(self) -> None:
        store = EvaluationStore()
        store.put("q1", {"evaluation_score": None, "retrieval_quality": "error", "query_id": "q1"})
        avg = store.average_score()
        assert avg is None


class TestEvaluationScoreIsValid:

    def test_valid_score(self) -> None:
        from orchestrator.app.rag_evaluator import is_valid_evaluation
        entry: Dict[str, Any] = {"evaluation_score": 0.85, "retrieval_quality": "good"}
        assert is_valid_evaluation(entry) is True

    def test_error_score(self) -> None:
        from orchestrator.app.rag_evaluator import is_valid_evaluation
        entry: Dict[str, Any] = {"evaluation_score": None, "retrieval_quality": "error"}
        assert is_valid_evaluation(entry) is False

    def test_skipped_score(self) -> None:
        from orchestrator.app.rag_evaluator import is_valid_evaluation
        entry: Dict[str, Any] = {"evaluation_score": None, "retrieval_quality": "skipped"}
        assert is_valid_evaluation(entry) is False

    def test_pending_score(self) -> None:
        from orchestrator.app.rag_evaluator import is_valid_evaluation
        entry: Dict[str, Any] = {"evaluation_score": None, "retrieval_quality": "pending"}
        assert is_valid_evaluation(entry) is False
