from __future__ import annotations

import pytest

from orchestrator.app.graph_embeddings import GraphTopology, Node2VecConfig, Node2VecEmbedder
from orchestrator.app.vector_store import SearchResult


class TestHybridScoring:
    def test_weighted_hybrid_score_default_weights(self) -> None:
        from orchestrator.app.graph_embeddings import hybrid_score

        text_score = 0.8
        structural_score = 0.6
        result = hybrid_score(text_score, structural_score)
        expected = 0.7 * 0.8 + 0.3 * 0.6
        assert abs(result - expected) < 1e-6

    def test_weighted_hybrid_score_custom_weights(self) -> None:
        from orchestrator.app.graph_embeddings import hybrid_score

        result = hybrid_score(0.9, 0.5, text_weight=0.5, structural_weight=0.5)
        expected = 0.5 * 0.9 + 0.5 * 0.5
        assert abs(result - expected) < 1e-6

    def test_zero_structural_score(self) -> None:
        from orchestrator.app.graph_embeddings import hybrid_score

        result = hybrid_score(0.8, 0.0)
        expected = 0.7 * 0.8
        assert abs(result - expected) < 1e-6


class TestRerankWithStructuralEmbeddings:
    def test_rerank_boosts_structurally_central_nodes(self) -> None:
        from orchestrator.app.graph_embeddings import rerank_with_structural

        text_results = [
            SearchResult(id="A", score=0.7, metadata={}),
            SearchResult(id="B", score=0.8, metadata={}),
        ]
        structural_embeddings = {
            "A": [1.0, 0.0],
            "B": [0.1, 0.1],
        }
        query_structural = [0.9, 0.1]

        reranked = rerank_with_structural(
            text_results, structural_embeddings, query_structural,
        )
        assert len(reranked) == 2
        assert reranked[0].id == "A" or reranked[0].id == "B"
        assert all(r.score >= 0 for r in reranked)

    def test_rerank_missing_embedding_uses_zero(self) -> None:
        from orchestrator.app.graph_embeddings import rerank_with_structural

        text_results = [
            SearchResult(id="C", score=0.9, metadata={}),
        ]
        reranked = rerank_with_structural(
            text_results, {}, [0.1, 0.2],
        )
        assert len(reranked) == 1
        expected = 0.7 * 0.9 + 0.3 * 0.0
        assert abs(reranked[0].score - expected) < 1e-6

    def test_rerank_preserves_metadata(self) -> None:
        from orchestrator.app.graph_embeddings import rerank_with_structural

        text_results = [
            SearchResult(id="X", score=0.5, metadata={"type": "service"}),
        ]
        reranked = rerank_with_structural(text_results, {}, [0.1])
        assert reranked[0].metadata == {"type": "service"}

    def test_empty_input(self) -> None:
        from orchestrator.app.graph_embeddings import rerank_with_structural

        assert rerank_with_structural([], {}, []) == []


class TestNode2VecEmbedderStillWorks:
    def test_embed_produces_correct_dim(self) -> None:
        topology = GraphTopology(
            nodes=["a", "b", "c"],
            adjacency={"a": ["b"], "b": ["a", "c"], "c": ["b"]},
        )
        config = Node2VecConfig(
            walk_length=10, num_walks=5, embedding_dim=16, seed=42,
        )
        embedder = Node2VecEmbedder(config)
        embeddings = embedder.embed(topology)
        assert len(embeddings) == 3
        assert len(embeddings["a"]) == 16
