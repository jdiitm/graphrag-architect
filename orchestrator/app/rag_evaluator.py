from __future__ import annotations

from dataclasses import dataclass
from typing import List

from orchestrator.app.vector_store import _cosine_similarity


@dataclass(frozen=True)
class RelevanceScore:
    query: str
    score: float
    context_count: int
    retrieval_path: str


def evaluate_relevance(
    query_embedding: List[float],
    context_embeddings: List[List[float]],
) -> float:
    if not context_embeddings:
        return 0.0
    total = sum(
        _cosine_similarity(query_embedding, ctx)
        for ctx in context_embeddings
    )
    return total / len(context_embeddings)


class RAGEvaluator:
    def __init__(self, low_relevance_threshold: float = 0.3) -> None:
        self._threshold = low_relevance_threshold

    def evaluate(
        self,
        query: str,
        query_embedding: List[float],
        context_embeddings: List[List[float]],
        retrieval_path: str = "vector",
    ) -> RelevanceScore:
        score = evaluate_relevance(query_embedding, context_embeddings)
        return RelevanceScore(
            query=query,
            score=score,
            context_count=len(context_embeddings),
            retrieval_path=retrieval_path,
        )

    def is_low_relevance(self, score: RelevanceScore) -> bool:
        return score.score < self._threshold
