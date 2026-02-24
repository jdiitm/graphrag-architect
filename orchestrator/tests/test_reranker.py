from __future__ import annotations

from orchestrator.app.reranker import (
    BM25Reranker,
    NoopReranker,
    ScoredCandidate,
    _bm25_score,
    _candidate_text,
    _tokenize,
)


class TestTokenize:
    def test_splits_on_word_boundaries(self) -> None:
        assert _tokenize("hello world") == ["hello", "world"]

    def test_lowercases_tokens(self) -> None:
        assert _tokenize("Hello WORLD") == ["hello", "world"]

    def test_empty_string(self) -> None:
        assert _tokenize("") == []


class TestBM25Score:
    def test_exact_match_positive_score(self) -> None:
        score = _bm25_score(["auth"], ["auth", "service"], avg_dl=5.0)
        assert score > 0

    def test_no_match_returns_zero(self) -> None:
        score = _bm25_score(["auth"], ["payments", "billing"], avg_dl=5.0)
        assert score == 0.0

    def test_empty_doc_returns_zero(self) -> None:
        assert _bm25_score(["auth"], [], avg_dl=5.0) == 0.0

    def test_zero_avg_dl_returns_zero(self) -> None:
        assert _bm25_score(["auth"], ["auth"], avg_dl=0.0) == 0.0


class TestCandidateText:
    def test_extracts_name_and_id(self) -> None:
        text = _candidate_text({"name": "auth-svc", "id": "svc-1"})
        assert "auth-svc" in text
        assert "svc-1" in text

    def test_handles_nested_result_dict(self) -> None:
        text = _candidate_text({"result": {"name": "inner-svc", "id": "x"}})
        assert "inner-svc" in text

    def test_empty_candidate_returns_empty(self) -> None:
        text = _candidate_text({})
        assert text == ""


class TestNoopReranker:
    def test_preserves_order(self) -> None:
        reranker = NoopReranker()
        candidates = [{"name": "a"}, {"name": "b"}]
        result = reranker.rerank("query", candidates)
        assert len(result) == 2
        assert result[0].data["name"] == "a"
        assert result[1].data["name"] == "b"

    def test_assigns_decreasing_scores(self) -> None:
        reranker = NoopReranker()
        candidates = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
        result = reranker.rerank("q", candidates)
        assert result[0].score > result[1].score > result[2].score


class TestBM25Reranker:
    def test_empty_candidates_returns_empty(self) -> None:
        reranker = BM25Reranker()
        assert reranker.rerank("auth service", []) == []

    def test_ranks_relevant_candidate_higher(self) -> None:
        reranker = BM25Reranker()
        candidates = [
            {"name": "billing-engine"},
            {"name": "auth-service"},
        ]
        result = reranker.rerank("auth service", candidates)
        assert result[0].data["name"] == "auth-service"

    def test_returns_scored_candidates(self) -> None:
        reranker = BM25Reranker()
        candidates = [{"name": "auth"}]
        result = reranker.rerank("auth", candidates)
        assert len(result) == 1
        assert isinstance(result[0], ScoredCandidate)
        assert result[0].score >= 0.0

    def test_sorted_by_score_descending(self) -> None:
        reranker = BM25Reranker()
        candidates = [
            {"name": "unrelated"},
            {"name": "auth-svc"},
            {"name": "auth-service-main"},
        ]
        result = reranker.rerank("auth service", candidates)
        scores = [r.score for r in result]
        assert scores == sorted(scores, reverse=True)
