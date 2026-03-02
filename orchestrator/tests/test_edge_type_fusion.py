from __future__ import annotations

import pytest

from orchestrator.app.graph_embeddings import (
    EdgeTypeWeights,
    edge_type_weighted_score,
    rerank_with_structural,
)
from orchestrator.app.vector_store import SearchResult


class TestEdgeTypeWeights:
    def test_defaults(self) -> None:
        w = EdgeTypeWeights()
        assert w.calls == 1.0
        assert w.produces == 0.8
        assert w.consumes == 0.8
        assert w.deployed_in == 0.5

    def test_frozen(self) -> None:
        w = EdgeTypeWeights()
        with pytest.raises(AttributeError):
            w.calls = 0.5

    def test_weight_for_known_type(self) -> None:
        w = EdgeTypeWeights()
        assert w.weight_for("CALLS") == 1.0
        assert w.weight_for("PRODUCES") == 0.8
        assert w.weight_for("DEPLOYED_IN") == 0.5

    def test_weight_for_unknown_type_returns_default(self) -> None:
        w = EdgeTypeWeights()
        assert w.weight_for("UNKNOWN_EDGE") == 1.0


class TestEdgeTypeWeightedScore:
    def test_calls_weighted_higher_than_deployed_in(self) -> None:
        base_score = 0.8
        calls_score = edge_type_weighted_score(base_score, "CALLS")
        deployed_score = edge_type_weighted_score(base_score, "DEPLOYED_IN")
        assert calls_score > deployed_score

    def test_missing_edge_type_uses_default(self) -> None:
        score = edge_type_weighted_score(0.5, None)
        assert score == pytest.approx(0.5, abs=1e-6)

    def test_zero_base_score_remains_zero(self) -> None:
        score = edge_type_weighted_score(0.0, "CALLS")
        assert score == 0.0


class TestRerankWithEdgeTypes:
    def test_edge_weights_applied_to_structural_scoring(self) -> None:
        text_results = [
            SearchResult(id="svc-a", score=0.9, metadata={"edge_type": "CALLS"}),
            SearchResult(id="svc-b", score=0.9, metadata={"edge_type": "DEPLOYED_IN"}),
        ]
        structural_embeddings = {
            "svc-a": [1.0, 0.0, 0.0],
            "svc-b": [1.0, 0.0, 0.0],
        }
        query_structural = [1.0, 0.0, 0.0]

        results = rerank_with_structural(
            text_results=text_results,
            structural_embeddings=structural_embeddings,
            query_structural=query_structural,
            edge_weights=EdgeTypeWeights(),
        )

        assert results[0].id == "svc-a"

    def test_rerank_without_edge_weights_unchanged(self) -> None:
        text_results = [
            SearchResult(id="svc-a", score=0.5, metadata={}),
            SearchResult(id="svc-b", score=0.9, metadata={}),
        ]
        structural_embeddings = {}
        query_structural = []

        results = rerank_with_structural(
            text_results=text_results,
            structural_embeddings=structural_embeddings,
            query_structural=query_structural,
        )

        assert results[0].id == "svc-b"
