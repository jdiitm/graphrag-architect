from __future__ import annotations

import math
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Protocol, runtime_checkable


class FusionStrategy(str, Enum):
    LINEAR = "linear"
    RRF = "rrf"


@dataclass
class ScoredCandidate:
    data: Dict[str, Any]
    score: float = 0.0
    candidate_id: str = ""


@runtime_checkable
class Reranker(Protocol):
    def rerank(
        self,
        query: str,
        candidates: List[Dict[str, Any]],
    ) -> List[ScoredCandidate]: ...


class NoopReranker:
    def rerank(
        self,
        query: str,
        candidates: List[Dict[str, Any]],
    ) -> List[ScoredCandidate]:
        return [
            ScoredCandidate(data=c, score=1.0 - i * 0.001)
            for i, c in enumerate(candidates)
        ]


_WORD_SPLIT = re.compile(r"\w+")


def _tokenize(text: str) -> List[str]:
    return [w.lower() for w in _WORD_SPLIT.findall(text)]


def _bm25_score(
    query_terms: List[str],
    doc_terms: List[str],
    avg_dl: float,
    k1: float = 1.2,
    b: float = 0.75,
) -> float:
    dl = len(doc_terms)
    if dl == 0 or avg_dl == 0:
        return 0.0
    tf_map: Dict[str, int] = {}
    for term in doc_terms:
        tf_map[term] = tf_map.get(term, 0) + 1
    score = 0.0
    for qt in query_terms:
        tf = tf_map.get(qt, 0)
        if tf == 0:
            continue
        numerator = tf * (k1 + 1)
        denominator = tf + k1 * (1 - b + b * dl / avg_dl)
        idf = math.log(2.0)
        score += idf * numerator / denominator
    return score


def _candidate_text(candidate: Dict[str, Any]) -> str:
    parts = []
    for key in ("name", "id", "source", "target", "result"):
        val = candidate.get(key)
        if val is not None:
            if isinstance(val, dict):
                parts.extend(str(v) for v in val.values() if v is not None)
            else:
                parts.append(str(val))
    return " ".join(parts)


@dataclass
class BM25Reranker:
    k1: float = 1.2
    b: float = 0.75

    def rerank(
        self,
        query: str,
        candidates: List[Dict[str, Any]],
    ) -> List[ScoredCandidate]:
        if not candidates:
            return []
        query_terms = _tokenize(query)
        doc_term_lists = [
            _tokenize(_candidate_text(c)) for c in candidates
        ]
        avg_dl = sum(len(d) for d in doc_term_lists) / max(len(doc_term_lists), 1)
        scored = []
        for candidate, doc_terms in zip(candidates, doc_term_lists):
            score = _bm25_score(
                query_terms, doc_terms, avg_dl,
                k1=self.k1, b=self.b,
            )
            scored.append(ScoredCandidate(data=candidate, score=score))
        scored.sort(key=lambda s: s.score, reverse=True)
        return scored


def reciprocal_rank_fusion(
    ranked_lists: List[List[ScoredCandidate]],
    k: int = 60,
) -> List[ScoredCandidate]:
    rrf_scores: Dict[str, float] = {}
    data_map: Dict[str, Dict[str, Any]] = {}
    for ranked_list in ranked_lists:
        for rank, candidate in enumerate(ranked_list):
            cid = candidate.candidate_id
            rrf_scores[cid] = rrf_scores.get(cid, 0.0) + 1.0 / (k + rank + 1)
            if cid not in data_map:
                data_map[cid] = candidate.data
    result = [
        ScoredCandidate(data=data_map[cid], score=score, candidate_id=cid)
        for cid, score in rrf_scores.items()
    ]
    result.sort(key=lambda s: s.score, reverse=True)
    return result
