from __future__ import annotations

import pytest

from orchestrator.app.graph_embeddings import (
    FusionWeightResolver,
    FusionWeights,
    rerank_with_structural,
)
from orchestrator.app.query_models import QueryComplexity
from orchestrator.app.vector_store import SearchResult


class TestFusionWeightsDataclass:

    def test_frozen(self) -> None:
        weights = FusionWeights(text=0.7, structural=0.3)
        with pytest.raises(AttributeError):
            weights.text = 0.5  # type: ignore[misc]

    def test_fields(self) -> None:
        weights = FusionWeights(text=0.6, structural=0.4)
        assert weights.text == 0.6
        assert weights.structural == 0.4


class TestFusionWeightResolverMapping:

    def test_entity_lookup(self) -> None:
        weights = FusionWeightResolver.resolve(QueryComplexity.ENTITY_LOOKUP)
        assert weights == FusionWeights(text=0.9, structural=0.1)

    def test_single_hop(self) -> None:
        weights = FusionWeightResolver.resolve(QueryComplexity.SINGLE_HOP)
        assert weights == FusionWeights(text=0.6, structural=0.4)

    def test_multi_hop(self) -> None:
        weights = FusionWeightResolver.resolve(QueryComplexity.MULTI_HOP)
        assert weights == FusionWeights(text=0.3, structural=0.7)

    def test_aggregate(self) -> None:
        weights = FusionWeightResolver.resolve(QueryComplexity.AGGREGATE)
        assert weights == FusionWeights(text=0.4, structural=0.6)

    def test_none_returns_legacy_weights(self) -> None:
        weights = FusionWeightResolver.resolve(None)
        assert weights == FusionWeights(text=0.7, structural=0.3)


class TestRerankWithComplexity:

    def test_rerank_accepts_complexity_parameter(self) -> None:
        text_results = [
            SearchResult(id="A", score=0.8, metadata={}),
            SearchResult(id="B", score=0.5, metadata={}),
        ]
        structural_embeddings = {
            "A": [0.0, 1.0],
            "B": [1.0, 0.0],
        }
        query_structural = [1.0, 0.0]

        reranked = rerank_with_structural(
            text_results, structural_embeddings, query_structural,
            complexity=QueryComplexity.MULTI_HOP,
        )
        assert len(reranked) == 2

    def test_multi_hop_favors_structural(self) -> None:
        text_results = [
            SearchResult(id="A", score=0.9, metadata={}),
            SearchResult(id="B", score=0.4, metadata={}),
        ]
        structural_embeddings = {
            "A": [0.0, 1.0],
            "B": [1.0, 0.0],
        }
        query_structural = [1.0, 0.0]

        reranked_multi = rerank_with_structural(
            text_results, structural_embeddings, query_structural,
            complexity=QueryComplexity.MULTI_HOP,
        )

        reranked_entity = rerank_with_structural(
            text_results, structural_embeddings, query_structural,
            complexity=QueryComplexity.ENTITY_LOOKUP,
        )

        assert reranked_multi[0].id == "B", (
            "MULTI_HOP (structural_weight=0.7) should rank B first "
            "due to structural alignment with query"
        )
        assert reranked_entity[0].id == "A", (
            "ENTITY_LOOKUP (text_weight=0.9) should rank A first "
            "due to higher text score"
        )

    def test_none_complexity_preserves_legacy_behavior(self) -> None:
        text_results = [
            SearchResult(id="X", score=0.6, metadata={}),
        ]
        structural_embeddings = {"X": [1.0, 0.0]}
        query_structural = [1.0, 0.0]

        reranked_none = rerank_with_structural(
            text_results, structural_embeddings, query_structural,
            complexity=None,
        )
        reranked_default = rerank_with_structural(
            text_results, structural_embeddings, query_structural,
        )

        assert abs(reranked_none[0].score - reranked_default[0].score) < 1e-6
