from __future__ import annotations

import pytest

from orchestrator.app.reranker import (
    FusionStrategy,
    ScoredCandidate,
    reciprocal_rank_fusion,
)
from orchestrator.app.vector_store import SearchResult


class TestReciprocalRankFusionMerge:
    def test_merges_two_ranked_lists_with_different_orderings(self) -> None:
        list_a = [
            ScoredCandidate(data={"id": "A"}, score=0.9, candidate_id="A"),
            ScoredCandidate(data={"id": "B"}, score=0.8, candidate_id="B"),
            ScoredCandidate(data={"id": "C"}, score=0.7, candidate_id="C"),
        ]
        list_b = [
            ScoredCandidate(data={"id": "C"}, score=0.9, candidate_id="C"),
            ScoredCandidate(data={"id": "A"}, score=0.8, candidate_id="A"),
            ScoredCandidate(data={"id": "B"}, score=0.7, candidate_id="B"),
        ]
        result = reciprocal_rank_fusion([list_a, list_b], k=60)
        assert len(result) == 3
        assert result[0].candidate_id == "A"

    def test_candidate_in_both_lists_outranks_single_list(self) -> None:
        list_a = [
            ScoredCandidate(data={"id": "X"}, score=0.9, candidate_id="X"),
            ScoredCandidate(data={"id": "Y"}, score=0.8, candidate_id="Y"),
        ]
        list_b = [
            ScoredCandidate(data={"id": "Y"}, score=0.9, candidate_id="Y"),
        ]
        result = reciprocal_rank_fusion([list_a, list_b], k=60)
        assert result[0].candidate_id == "Y"


class TestReciprocalRankFusionEdgeCases:
    def test_single_input_list_preserves_original_ranking(self) -> None:
        single = [
            ScoredCandidate(data={"id": "A"}, score=0.9, candidate_id="A"),
            ScoredCandidate(data={"id": "B"}, score=0.5, candidate_id="B"),
            ScoredCandidate(data={"id": "C"}, score=0.1, candidate_id="C"),
        ]
        result = reciprocal_rank_fusion([single], k=60)
        assert [r.candidate_id for r in result] == ["A", "B", "C"]

    def test_no_input_lists_returns_empty(self) -> None:
        assert reciprocal_rank_fusion([], k=60) == []

    def test_all_empty_sublists_returns_empty(self) -> None:
        assert reciprocal_rank_fusion([[], []], k=60) == []


class TestReciprocalRankFusionScoring:
    def test_k_parameter_affects_score_spread(self) -> None:
        ranked = [
            ScoredCandidate(data={"id": "A"}, score=0.9, candidate_id="A"),
            ScoredCandidate(data={"id": "B"}, score=0.1, candidate_id="B"),
        ]
        result_k1 = reciprocal_rank_fusion([ranked], k=1)
        result_k60 = reciprocal_rank_fusion([ranked], k=60)
        spread_k1 = result_k1[0].score - result_k1[1].score
        spread_k60 = result_k60[0].score - result_k60[1].score
        assert spread_k1 > spread_k60

    def test_rrf_scores_always_positive(self) -> None:
        ranked = [
            ScoredCandidate(data={"id": "A"}, score=-5.0, candidate_id="A"),
            ScoredCandidate(data={"id": "B"}, score=0.0, candidate_id="B"),
        ]
        result = reciprocal_rank_fusion([ranked], k=60)
        for candidate in result:
            assert candidate.score > 0.0

    def test_all_candidate_ids_preserved_across_lists(self) -> None:
        list_a = [
            ScoredCandidate(data={"id": "A"}, score=0.9, candidate_id="A"),
            ScoredCandidate(data={"id": "B"}, score=0.8, candidate_id="B"),
        ]
        list_b = [
            ScoredCandidate(data={"id": "B"}, score=0.9, candidate_id="B"),
            ScoredCandidate(data={"id": "C"}, score=0.8, candidate_id="C"),
        ]
        result = reciprocal_rank_fusion([list_a, list_b], k=60)
        assert {r.candidate_id for r in result} == {"A", "B", "C"}


class TestFusionStrategyEnum:
    def test_rrf_value(self) -> None:
        assert FusionStrategy.RRF.value == "rrf"

    def test_linear_value(self) -> None:
        assert FusionStrategy.LINEAR.value == "linear"


class TestFusionStrategyInRerankWithStructural:
    def test_rrf_strategy_returns_all_candidates(self) -> None:
        from orchestrator.app.graph_embeddings import rerank_with_structural

        text_results = [
            SearchResult(id="A", score=0.9, metadata={"name": "A"}),
            SearchResult(id="B", score=0.5, metadata={"name": "B"}),
            SearchResult(id="C", score=0.1, metadata={"name": "C"}),
        ]
        structural_embeddings = {
            "A": [0.0, 1.0],
            "B": [0.5, 0.5],
            "C": [1.0, 0.0],
        }
        query_structural = [1.0, 0.0]
        result = rerank_with_structural(
            text_results, structural_embeddings, query_structural,
            fusion_strategy="rrf",
        )
        assert len(result) == 3
        assert {r.id for r in result} == {"A", "B", "C"}

    def test_linear_strategy_matches_default_behavior(self) -> None:
        from orchestrator.app.graph_embeddings import rerank_with_structural

        text_results = [
            SearchResult(id="A", score=0.6, metadata={}),
            SearchResult(id="B", score=0.9, metadata={}),
        ]
        structural_embeddings = {
            "A": [1.0, 0.0],
            "B": [0.0, 1.0],
        }
        query_structural = [1.0, 0.0]
        result_default = rerank_with_structural(
            text_results, structural_embeddings, query_structural,
        )
        result_linear = rerank_with_structural(
            text_results, structural_embeddings, query_structural,
            fusion_strategy="linear",
        )
        assert [r.id for r in result_default] == [r.id for r in result_linear]
