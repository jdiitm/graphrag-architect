from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from orchestrator.app.reranker import BM25Reranker, ScoredCandidate
from orchestrator.app.density_reranker import DensityReranker


def _sample_candidates() -> List[Dict[str, Any]]:
    return [
        {"name": "auth-service", "id": "auth-1", "score": 0.9},
        {"name": "user-service", "id": "user-1", "score": 0.7},
        {"name": "payment-service", "id": "pay-1", "score": 0.5},
        {"name": "order-service", "id": "order-1", "score": 0.3},
    ]


class TestAsyncRerankOffload:

    @pytest.mark.asyncio
    async def test_bm25_reranking_runs_off_event_loop_thread(self):
        from orchestrator.app.query_engine import _async_rerank_candidates

        event_loop_thread = threading.current_thread()
        captured_threads: List[threading.Thread] = []

        original_rerank = BM25Reranker.rerank

        def capturing_rerank(
            self: BM25Reranker,
            query: str,
            candidates: List[Dict[str, Any]],
        ) -> List[ScoredCandidate]:
            captured_threads.append(threading.current_thread())
            return original_rerank(self, query, candidates)

        candidates = _sample_candidates()

        with patch.object(BM25Reranker, "rerank", capturing_rerank), patch(
            "orchestrator.app.query_engine._DENSITY_CFG",
            type("Cfg", (), {"enable_density_rerank": False})(),
        ), patch(
            "orchestrator.app.query_engine._STRUCTURAL_EMBEDDINGS", {},
        ):
            await _async_rerank_candidates(
                "find auth service", candidates,
            )

        assert len(captured_threads) == 1
        assert captured_threads[0] is not event_loop_thread

    @pytest.mark.asyncio
    async def test_density_reranking_runs_off_event_loop_thread(self):
        from orchestrator.app.query_engine import _async_rerank_candidates

        event_loop_thread = threading.current_thread()
        captured_threads: List[threading.Thread] = []

        original_rerank = DensityReranker.rerank

        def capturing_rerank(
            self: DensityReranker,
            query: str,
            candidates: List[Dict[str, Any]],
        ) -> List[ScoredCandidate]:
            captured_threads.append(threading.current_thread())
            return original_rerank(self, query, candidates)

        candidates = _sample_candidates()

        with (
            patch(
                "orchestrator.app.query_engine._DENSITY_CFG",
                type("Cfg", (), {
                    "enable_density_rerank": True,
                    "lambda_param": 0.7,
                    "min_candidates": 3,
                })(),
            ),
            patch.object(DensityReranker, "rerank", capturing_rerank),
        ):
            await _async_rerank_candidates(
                "find auth service", candidates,
            )

        assert len(captured_threads) >= 1
        assert all(t is not event_loop_thread for t in captured_threads)

    @pytest.mark.asyncio
    async def test_async_rerank_results_match_sync_bm25(self):
        from orchestrator.app.query_engine import _async_rerank_candidates

        candidates = _sample_candidates()
        query = "authentication service dependencies"

        reranker = BM25Reranker()
        sync_result = [sc.data for sc in reranker.rerank(query, candidates)]

        with patch(
            "orchestrator.app.query_engine._DENSITY_CFG",
            type("Cfg", (), {"enable_density_rerank": False})(),
        ), patch(
            "orchestrator.app.query_engine._STRUCTURAL_EMBEDDINGS", {},
        ):
            async_result = await _async_rerank_candidates(query, candidates)

        assert async_result == sync_result

    @pytest.mark.asyncio
    async def test_async_rerank_empty_candidates(self):
        from orchestrator.app.query_engine import _async_rerank_candidates

        with patch(
            "orchestrator.app.query_engine._DENSITY_CFG",
            type("Cfg", (), {"enable_density_rerank": False})(),
        ), patch(
            "orchestrator.app.query_engine._STRUCTURAL_EMBEDDINGS", {},
        ):
            result = await _async_rerank_candidates("query", [])

        assert result == []

    @pytest.mark.asyncio
    async def test_concurrent_reranking_does_not_serialize(self):
        from orchestrator.app.query_engine import _async_rerank_candidates

        candidates = _sample_candidates()
        barrier = threading.Barrier(2, timeout=5)
        call_count = 0
        lock = threading.Lock()

        original_rerank = BM25Reranker.rerank

        def slow_rerank(
            self: BM25Reranker,
            query: str,
            cands: List[Dict[str, Any]],
        ) -> List[ScoredCandidate]:
            nonlocal call_count
            with lock:
                call_count += 1
            barrier.wait()
            return original_rerank(self, query, cands)

        with patch.object(BM25Reranker, "rerank", slow_rerank), patch(
            "orchestrator.app.query_engine._DENSITY_CFG",
            type("Cfg", (), {"enable_density_rerank": False})(),
        ), patch(
            "orchestrator.app.query_engine._STRUCTURAL_EMBEDDINGS", {},
        ):
            results = await asyncio.gather(
                _async_rerank_candidates("query A", candidates),
                _async_rerank_candidates("query B", candidates),
            )

        assert call_count == 2
        assert len(results) == 2
        assert all(len(r) == len(candidates) for r in results)

    @pytest.mark.asyncio
    async def test_structural_rerank_offloaded_to_thread(self):
        from orchestrator.app.query_engine import _async_rerank_candidates

        event_loop_thread = threading.current_thread()
        structural_thread = None

        from orchestrator.app.query_engine import _apply_structural_rerank
        original_fn = _apply_structural_rerank

        def capturing_structural(
            candidates: List[Dict[str, Any]],
            complexity: Any = None,
        ) -> List[Dict[str, Any]]:
            nonlocal structural_thread
            structural_thread = threading.current_thread()
            return candidates

        embeddings = {"auth-1": [0.1] * 128}

        with patch(
            "orchestrator.app.query_engine._STRUCTURAL_EMBEDDINGS",
            embeddings,
        ), patch(
            "orchestrator.app.query_engine._apply_structural_rerank",
            capturing_structural,
        ), patch(
            "orchestrator.app.query_engine._DENSITY_CFG",
            type("Cfg", (), {"enable_density_rerank": False})(),
        ):
            await _async_rerank_candidates(
                "auth service", _sample_candidates(),
            )

        assert structural_thread is not None
        assert structural_thread is not event_loop_thread


class TestRerankThreadPoolLifecycle:

    def test_get_rerank_thread_pool_returns_executor(self):
        from orchestrator.app.executor import get_thread_pool

        pool = get_thread_pool()
        assert isinstance(pool, ThreadPoolExecutor)

    def test_shutdown_thread_pool_clears_instance(self):
        from orchestrator.app.executor import (
            get_thread_pool,
            shutdown_thread_pool,
        )

        pool = get_thread_pool()
        assert pool is not None
        shutdown_thread_pool()

        new_pool = get_thread_pool()
        assert new_pool is not pool
