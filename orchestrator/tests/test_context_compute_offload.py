import threading
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.context_manager import TokenBudget, truncate_context_topology


def _make_state(base_query_state, **overrides):
    return {**base_query_state, **overrides}


class TestContextComputeOffload:

    @pytest.mark.asyncio
    async def test_truncate_runs_off_event_loop_thread(self, base_query_state):
        from orchestrator.app.query_engine import _do_synthesize

        event_loop_thread = threading.current_thread()
        execution_threads: list[threading.Thread] = []

        original_truncate = truncate_context_topology

        def tracking_truncate(candidates, budget):
            execution_threads.append(threading.current_thread())
            return original_truncate(candidates, budget)

        state = _make_state(
            base_query_state,
            candidates=[{"name": "svc-a", "score": 0.9}],
        )

        with patch(
            "orchestrator.app.query_engine.truncate_context_topology",
            side_effect=tracking_truncate,
        ), patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="test answer",
        ):
            await _do_synthesize(state)

        assert len(execution_threads) == 1, (
            "truncate_context_topology must be called exactly once"
        )
        assert execution_threads[0] is not event_loop_thread, (
            "truncate_context_topology must run in a thread pool thread, "
            "not the event loop thread"
        )

    @pytest.mark.asyncio
    async def test_truncate_uses_shared_thread_pool(self, base_query_state):
        from orchestrator.app.query_engine import _do_synthesize
        from orchestrator.app.executor import get_thread_pool as real_get_pool

        pool_call_count = [0]

        def counting_get_pool():
            pool_call_count[0] += 1
            return real_get_pool()

        state = _make_state(
            base_query_state,
            candidates=[{"name": "svc-a", "score": 0.9}],
        )

        with patch(
            "orchestrator.app.query_engine.get_thread_pool",
            side_effect=counting_get_pool,
        ), patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="test answer",
        ):
            await _do_synthesize(state)

        assert pool_call_count[0] >= 2, (
            f"get_thread_pool called {pool_call_count[0]} time(s); "
            "expected >= 2 (rerank + truncation offload)"
        )

    @pytest.mark.asyncio
    async def test_behavioral_equivalence(self, base_query_state):
        from orchestrator.app.query_engine import _do_synthesize

        candidates = [
            {"name": "auth-service", "source": "auth", "target": "user", "score": 0.9},
            {"name": "user-service", "source": "user", "target": "db", "score": 0.7},
        ]

        state = _make_state(
            base_query_state,
            candidates=candidates,
        )

        with patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="synthesized answer",
        ) as mock_llm:
            result = await _do_synthesize(state)

        assert result["answer"] == "synthesized answer"
        assert len(result["sources"]) > 0

        llm_context = mock_llm.call_args[0][1]
        source_names = {s.get("name") for s in llm_context}
        input_names = {c["name"] for c in candidates}
        assert source_names <= input_names, (
            f"Truncated context names {source_names} must be a subset of "
            f"input candidates {input_names}"
        )

    @pytest.mark.asyncio
    async def test_fallback_on_executor_failure(self, base_query_state):
        from orchestrator.app.query_engine import _do_synthesize

        state = _make_state(
            base_query_state,
            candidates=[{"name": "svc-a", "score": 0.9}],
        )

        with patch(
            "orchestrator.app.query_engine.truncate_context_topology",
            side_effect=RuntimeError("Thread pool exhausted"),
        ), patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="fallback answer",
        ):
            result = await _do_synthesize(state)

        assert result["answer"] == "fallback answer"
        assert len(result["sources"]) > 0, (
            "On executor failure, untruncated context must be used as fallback"
        )
