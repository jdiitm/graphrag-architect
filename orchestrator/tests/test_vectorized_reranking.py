from __future__ import annotations

import time
from typing import Any, Dict, List

import pytest

from orchestrator.app.vector_store import SearchResult


class TestVectorizedCosineSimilarity:

    def test_identical_vectors_score_one(self) -> None:
        from orchestrator.app.semantic_cache import _cosine_similarity
        v = [1.0, 0.0, 0.0]
        assert _cosine_similarity(v, v) == pytest.approx(1.0)

    def test_orthogonal_vectors_score_zero(self) -> None:
        from orchestrator.app.semantic_cache import _cosine_similarity
        assert _cosine_similarity([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)

    def test_vector_store_cosine_identical(self) -> None:
        from orchestrator.app.vector_store import _cosine_similarity
        v = [1.0, 0.0, 0.5]
        assert _cosine_similarity(v, v) == pytest.approx(1.0)

    def test_vector_store_cosine_different_lengths_raises(self) -> None:
        from orchestrator.app.vector_store import _cosine_similarity
        with pytest.raises(ValueError):
            _cosine_similarity([1.0], [1.0, 2.0])

    def test_graph_embeddings_cosine_identical(self) -> None:
        from orchestrator.app.graph_embeddings import _cosine_sim
        v = [1.0, 0.0, 0.5]
        assert _cosine_sim(v, v) == pytest.approx(1.0)


class TestVectorizedReranking:

    def _make_results(self, n: int, dim: int) -> tuple:
        import random
        rng = random.Random(42)
        embeddings: Dict[str, List[float]] = {}
        results: List[SearchResult] = []
        for i in range(n):
            node_id = f"node-{i}"
            vec = [rng.gauss(0, 1) for _ in range(dim)]
            embeddings[node_id] = vec
            results.append(SearchResult(
                id=node_id,
                score=rng.random(),
                metadata={"name": node_id},
            ))
        query_structural = [rng.gauss(0, 1) for _ in range(dim)]
        return results, embeddings, query_structural

    def test_rerank_correctness_small(self) -> None:
        from orchestrator.app.graph_embeddings import rerank_with_structural

        results, embeddings, query = self._make_results(10, 128)
        reranked = rerank_with_structural(results, embeddings, query)

        assert len(reranked) == 10
        scores = [r.score for r in reranked]
        assert scores == sorted(scores, reverse=True)

    def test_rerank_preserves_all_ids(self) -> None:
        from orchestrator.app.graph_embeddings import rerank_with_structural

        results, embeddings, query = self._make_results(50, 128)
        reranked = rerank_with_structural(results, embeddings, query)

        original_ids = {r.id for r in results}
        reranked_ids = {r.id for r in reranked}
        assert original_ids == reranked_ids

    def test_rerank_empty_candidates(self) -> None:
        from orchestrator.app.graph_embeddings import rerank_with_structural
        reranked = rerank_with_structural([], {}, [])
        assert reranked == []

    def test_rerank_single_candidate(self) -> None:
        from orchestrator.app.graph_embeddings import rerank_with_structural

        results = [SearchResult(id="a", score=0.5, metadata={"name": "a"})]
        embeddings = {"a": [1.0, 0.0]}
        query = [0.5, 0.5]
        reranked = rerank_with_structural(results, embeddings, query)
        assert len(reranked) == 1

    def test_rerank_performance_10k_candidates(self) -> None:
        from orchestrator.app.graph_embeddings import rerank_with_structural

        results, embeddings, query = self._make_results(10_000, 128)

        start = time.monotonic()
        reranked = rerank_with_structural(results, embeddings, query)
        elapsed_ms = (time.monotonic() - start) * 1000

        assert len(reranked) == 10_000
        assert elapsed_ms < 500, (
            f"Reranking 10k candidates took {elapsed_ms:.0f}ms, must be <500ms"
        )


class TestVectorizedCentroid:

    def test_centroid_calculation_correctness(self) -> None:
        from orchestrator.app.graph_embeddings import compute_centroid

        vectors = [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
        ]
        centroid = compute_centroid(vectors)
        expected = [1 / 3, 1 / 3, 1 / 3]
        for actual, exp in zip(centroid, expected):
            assert actual == pytest.approx(exp, abs=1e-6)

    def test_centroid_single_vector(self) -> None:
        from orchestrator.app.graph_embeddings import compute_centroid

        vectors = [[1.0, 2.0, 3.0]]
        centroid = compute_centroid(vectors)
        assert centroid == pytest.approx([1.0, 2.0, 3.0])

    def test_centroid_empty_returns_empty(self) -> None:
        from orchestrator.app.graph_embeddings import compute_centroid

        centroid = compute_centroid([])
        assert centroid == []
