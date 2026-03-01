from __future__ import annotations

import pytest

from orchestrator.app.rag_evaluator import (
    _compute_groundedness,
    _compute_semantic_groundedness,
)


class TestSemanticGroundedness:

    def test_known_entities_score_above_baseline(self) -> None:
        score = _compute_semantic_groundedness(
            answer="Auth depends on Postgres",
            sources=[
                {"name": "Auth", "type": "Service"},
                {"name": "Postgres", "type": "Database"},
            ],
        )
        assert score >= 0.5

    def test_synonym_match_scores_higher_than_lexical(self) -> None:
        semantic = _compute_semantic_groundedness(
            answer="The Database Layer stores user records",
            sources=[{"name": "DB", "type": "Database", "stores": ["user records"]}],
        )
        lexical = _compute_groundedness(
            answer="The Database Layer stores user records",
            sources=[{"name": "DB", "type": "Database", "stores": ["user records"]}],
        )
        assert semantic >= lexical, (
            f"Semantic groundedness ({semantic:.2f}) must score at least as "
            f"high as lexical ({lexical:.2f}) for synonym matches"
        )

    def test_empty_answer_returns_full_score(self) -> None:
        assert _compute_semantic_groundedness("", []) == 1.0

    def test_hallucinated_entity_lowers_score(self) -> None:
        score = _compute_semantic_groundedness(
            answer="The FooBar service causes cascading failures",
            sources=[{"name": "Auth", "type": "Service"}],
        )
        assert score < 0.8


class TestLexicalGroundednessImproved:

    def test_case_insensitive_matching(self) -> None:
        score = _compute_groundedness(
            answer="The AUTH service handles authentication",
            sources=[{"name": "auth", "type": "service"}],
        )
        assert score > 0.0, "Groundedness must be case-insensitive"

    def test_no_hardcoded_stop_word_dependency(self) -> None:
        score = _compute_groundedness(
            answer="database layer service",
            sources=[{"name": "database", "type": "layer", "role": "service"}],
        )
        assert score >= 0.5
