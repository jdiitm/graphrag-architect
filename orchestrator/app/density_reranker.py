from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Set

from orchestrator.app.reranker import (
    BM25Reranker,
    ScoredCandidate,
    _tokenize,
)


def _full_candidate_text(candidate: Dict[str, Any]) -> str:
    parts: List[str] = []
    for value in candidate.values():
        if value is None:
            continue
        if isinstance(value, dict):
            parts.extend(str(v) for v in value.values() if v is not None)
        else:
            parts.append(str(value))
    return " ".join(parts)


def _jaccard_similarity(tokens_a: Set[str], tokens_b: Set[str]) -> float:
    if not tokens_a and not tokens_b:
        return 1.0
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = len(tokens_a & tokens_b)
    union = len(tokens_a | tokens_b)
    return intersection / union


@dataclass(frozen=True)
class DensityRerankerConfig:
    lambda_param: float = 0.7
    min_candidates: int = 3
    enable_density_rerank: bool = True

    @classmethod
    def from_env(cls) -> DensityRerankerConfig:
        lambda_param = float(os.environ.get("DENSITY_RERANK_LAMBDA", "0.7"))
        min_candidates = int(os.environ.get("DENSITY_RERANK_MIN_CANDIDATES", "3"))
        enabled_raw = os.environ.get("DENSITY_RERANK_ENABLED", "true")
        enable_density_rerank = enabled_raw.lower() in ("true", "1", "yes")
        return cls(
            lambda_param=lambda_param,
            min_candidates=min_candidates,
            enable_density_rerank=enable_density_rerank,
        )


def _mmr_select(
    token_sets: List[Set[str]],
    normalized: List[float],
    lambda_param: float,
) -> List[int]:
    selected: List[int] = []
    remaining = set(range(len(normalized)))

    for _ in range(len(normalized)):
        best_idx = -1
        best_key = (float("-inf"), float("-inf"), len(normalized))

        for idx in remaining:
            max_sim = max(
                (_jaccard_similarity(token_sets[idx], token_sets[s])
                 for s in selected),
                default=0.0,
            )
            mmr = lambda_param * normalized[idx] - (1.0 - lambda_param) * max_sim
            key = (mmr, normalized[idx], -idx)
            if key > best_key:
                best_key = key
                best_idx = idx

        selected.append(best_idx)
        remaining.discard(best_idx)

    return selected


@dataclass
class DensityReranker:
    lambda_param: float = 0.7
    min_candidates: int = 3

    def rerank(
        self,
        query: str,
        candidates: List[Dict[str, Any]],
    ) -> List[ScoredCandidate]:
        if not candidates:
            return []

        scored = BM25Reranker().rerank(query, candidates)

        if len(scored) < self.min_candidates:
            return scored

        token_sets: List[Set[str]] = [
            set(_tokenize(_full_candidate_text(sc.data)))
            for sc in scored
        ]
        max_score = max(sc.score for sc in scored)
        normalized = [
            sc.score / max_score if max_score > 0 else 0.0
            for sc in scored
        ]

        order = _mmr_select(token_sets, normalized, self.lambda_param)
        return [scored[i] for i in order]
