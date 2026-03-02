import pytest

from orchestrator.app.graph_embeddings import (
    FusionHint,
    FusionWeightResolver,
    classify_fusion_hint,
    rerank_with_structural,
)
from orchestrator.app.vector_store import SearchResult


def test_structural_query_high_structural_weight():
    hint = classify_fusion_hint("what services are connected to auth-service")
    assert hint is not None
    assert hint.structural_weight > 0.5


def test_descriptive_query_high_text_weight():
    hint = classify_fusion_hint("describe the auth-service")
    assert hint is not None
    assert hint.text_weight > 0.5


def test_fallback_to_static_when_no_hint():
    hint = classify_fusion_hint("auth-service")
    assert hint is None
    weights = FusionWeightResolver.resolve(hint=hint)
    assert weights.text == 0.7
    assert weights.structural == 0.3


def test_weights_sum_to_one():
    queries = [
        "what services depend on auth",
        "describe the payment service",
        "topology of the messaging system",
        "what does the api gateway do",
    ]
    for query in queries:
        hint = classify_fusion_hint(query)
        if hint is not None:
            assert abs(hint.text_weight + hint.structural_weight - 1.0) < 1e-9


def test_hint_propagates_to_reranking():
    results = [
        SearchResult(id="svc-a", score=0.5, metadata={"name": "svc-a"}),
        SearchResult(id="svc-b", score=0.9, metadata={"name": "svc-b"}),
    ]
    embeddings = {
        "svc-a": [1.0, 0.0, 0.0],
        "svc-b": [0.0, 0.0, 1.0],
    }
    query_structural = [1.0, 0.0, 0.0]
    hint = FusionHint(text_weight=0.2, structural_weight=0.8)

    reranked = rerank_with_structural(
        results, embeddings, query_structural, hint=hint,
    )

    assert len(reranked) == 2
    assert reranked[0].id == "svc-a"
