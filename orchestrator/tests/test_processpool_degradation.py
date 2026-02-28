from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
)


_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


class TestLocalProcessPoolFallbackRemoved:

    @pytest.mark.asyncio
    async def test_remote_ast_true_does_not_create_process_pool(self) -> None:
        from orchestrator.app.graph_builder import (
            IngestionDegradedError,
            parse_source_ast,
        )

        state = {
            "raw_files": [
                {"path": "services/auth/main.go", "content": "package main"},
            ],
            "extracted_nodes": [],
            "extraction_checkpoint": {},
            "tenant_id": "test-tenant",
        }

        with (
            patch.dict("os.environ", {**_ENV_VARS, "USE_REMOTE_AST": "true"}),
            patch(
                "orchestrator.app.graph_builder._USE_REMOTE_AST",
                True,
            ),
            patch(
                "orchestrator.app.graph_builder._get_process_pool",
                side_effect=AssertionError(
                    "ProcessPoolExecutor must not be created when USE_REMOTE_AST=true"
                ),
            ),
            patch(
                "orchestrator.app.graph_builder._ast_worker_breaker",
            ) as mock_breaker,
        ):
            mock_breaker.call = AsyncMock(
                side_effect=ConnectionError("Go worker unreachable"),
            )
            mock_breaker._config = MagicMock(recovery_timeout=30.0)
            with pytest.raises(IngestionDegradedError):
                await parse_source_ast(state)

    @pytest.mark.asyncio
    async def test_remote_ast_false_allows_process_pool(self) -> None:
        from orchestrator.app.graph_builder import parse_source_ast
        from orchestrator.app.ast_extraction import ASTExtractionResult

        empty_result = ASTExtractionResult()

        state = {
            "raw_files": [
                {"path": "services/auth/main.go", "content": "package main"},
            ],
            "extracted_nodes": [],
            "extraction_checkpoint": {},
            "tenant_id": "test-tenant",
        }

        mock_pool = MagicMock()

        async def _fake_run_in_executor(pool, fn, *args):
            return fn(*args)

        mock_loop = MagicMock()
        mock_loop.run_in_executor = _fake_run_in_executor

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder._USE_REMOTE_AST",
                False,
            ),
            patch(
                "asyncio.get_running_loop",
                return_value=mock_loop,
            ),
        ):
            result = await parse_source_ast(state)

        assert "extracted_nodes" in result


class TestDegradedError503WithRetryAfter:

    @pytest.mark.asyncio
    async def test_degraded_error_contains_retry_after(self) -> None:
        from orchestrator.app.graph_builder import IngestionDegradedError

        err = IngestionDegradedError(
            "Go workers unavailable", retry_after_seconds=60,
        )
        assert err.retry_after_seconds == 60
        assert "Go workers unavailable" in str(err)

    @pytest.mark.asyncio
    async def test_ingest_sync_returns_503_on_degraded_error(self) -> None:
        from orchestrator.app.graph_builder import IngestionDegradedError
        from orchestrator.app.main import _ingest_sync

        degraded = IngestionDegradedError(
            "Go workers unavailable", retry_after_seconds=30,
        )

        mock_sem = AsyncMock()
        mock_sem.try_acquire = AsyncMock(return_value=(True, "tok"))
        mock_sem.release = AsyncMock()

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.main.get_ingestion_semaphore",
                return_value=mock_sem,
            ),
            patch(
                "orchestrator.app.main.ingestion_graph",
            ) as mock_graph,
        ):
            mock_graph.ainvoke = AsyncMock(side_effect=degraded)
            response = await _ingest_sync([])

        assert response.status_code == 503
        assert response.headers.get("Retry-After") == "30"

    @pytest.mark.asyncio
    async def test_ingest_async_fails_job_on_degraded_error(self) -> None:
        from orchestrator.app.graph_builder import IngestionDegradedError
        from orchestrator.app.main import _run_ingest_job

        degraded = IngestionDegradedError(
            "Go workers unavailable", retry_after_seconds=30,
        )

        mock_sem = AsyncMock()
        mock_sem.try_acquire = AsyncMock(return_value=(True, "tok"))
        mock_sem.release = AsyncMock()
        mock_store = AsyncMock()

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.main.get_ingestion_semaphore",
                return_value=mock_sem,
            ),
            patch(
                "orchestrator.app.main._INGEST_JOB_STORE",
                mock_store,
            ),
            patch(
                "orchestrator.app.main.ingestion_graph",
            ) as mock_graph,
        ):
            mock_graph.ainvoke = AsyncMock(side_effect=degraded)
            await _run_ingest_job("job-1", [])

        mock_store.fail.assert_called_once()
        fail_args = mock_store.fail.call_args
        assert "job-1" in fail_args[0]


class TestCircuitBreakerWrapsASTWorkerCalls:

    @pytest.mark.asyncio
    async def test_circuit_breaker_trips_after_repeated_failures(self) -> None:
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=2,
                recovery_timeout=30.0,
                jitter_factor=0.0,
            ),
            name="ast-worker-test",
        )

        async def _failing_call():
            raise ConnectionError("Go worker unreachable")

        for _ in range(2):
            with pytest.raises(ConnectionError):
                await breaker.call(_failing_call)

        with pytest.raises(CircuitOpenError):
            await breaker.call(_failing_call)

    @pytest.mark.asyncio
    async def test_ast_worker_breaker_is_used_in_parse_source_ast(self) -> None:
        from orchestrator.app.graph_builder import IngestionDegradedError

        state = {
            "raw_files": [
                {"path": "services/auth/main.go", "content": "package main"},
            ],
            "extracted_nodes": [],
            "extraction_checkpoint": {},
            "tenant_id": "test-tenant",
        }

        with (
            patch.dict("os.environ", {**_ENV_VARS, "USE_REMOTE_AST": "true"}),
            patch(
                "orchestrator.app.graph_builder._USE_REMOTE_AST",
                True,
            ),
            patch(
                "orchestrator.app.graph_builder._ast_worker_breaker",
            ) as mock_breaker,
        ):
            mock_breaker.call = AsyncMock(
                side_effect=CircuitOpenError("circuit open"),
            )
            with pytest.raises(IngestionDegradedError):
                from orchestrator.app.graph_builder import parse_source_ast
                await parse_source_ast(state)


class TestDeadLetterQueueForFailedJobs:

    @pytest.fixture(autouse=True)
    def _clear_dlq(self) -> None:
        import orchestrator.app.graph_builder as gb
        gb._AST_DLQ.clear()

    @pytest.mark.asyncio
    async def test_degraded_ast_job_enqueued_to_dlq(self) -> None:
        from orchestrator.app.graph_builder import (
            enqueue_ast_dlq,
            get_ast_dlq,
        )

        payload = {
            "raw_files": [
                {"path": "main.go", "content": "package main"},
            ],
            "tenant_id": "test-tenant",
        }

        enqueue_ast_dlq(payload)
        dlq = get_ast_dlq()
        assert len(dlq) == 1
        assert dlq[0]["raw_files"][0]["path"] == "main.go"

    @pytest.mark.asyncio
    async def test_parse_source_ast_enqueues_dlq_on_degradation(self) -> None:
        from orchestrator.app.graph_builder import (
            IngestionDegradedError,
            get_ast_dlq,
            parse_source_ast,
        )

        state = {
            "raw_files": [
                {"path": "services/auth/main.go", "content": "package main"},
            ],
            "extracted_nodes": [],
            "extraction_checkpoint": {},
            "tenant_id": "test-tenant",
        }

        with (
            patch.dict("os.environ", {**_ENV_VARS, "USE_REMOTE_AST": "true"}),
            patch(
                "orchestrator.app.graph_builder._USE_REMOTE_AST",
                True,
            ),
            patch(
                "orchestrator.app.graph_builder._ast_worker_breaker",
            ) as mock_breaker,
        ):
            mock_breaker.call = AsyncMock(
                side_effect=ConnectionError("Go worker unreachable"),
            )
            mock_breaker._config = MagicMock(recovery_timeout=30.0)
            with pytest.raises(IngestionDegradedError):
                await parse_source_ast(state)

        dlq = get_ast_dlq()
        assert len(dlq) == 1
        assert dlq[0]["tenant_id"] == "test-tenant"

    def test_dlq_is_bounded(self) -> None:
        import orchestrator.app.graph_builder as gb

        assert hasattr(gb._AST_DLQ, "maxlen"), (
            "DLQ must be a bounded collection (e.g. collections.deque)"
        )
        assert gb._AST_DLQ.maxlen is not None
        assert gb._AST_DLQ.maxlen > 0

    def test_dlq_evicts_oldest_when_full(self) -> None:
        from collections import deque

        from orchestrator.app.graph_builder import (
            enqueue_ast_dlq,
            get_ast_dlq,
        )
        import orchestrator.app.graph_builder as gb

        original = gb._AST_DLQ
        gb._AST_DLQ = deque(maxlen=3)
        try:
            for i in range(5):
                enqueue_ast_dlq({"index": i})
            dlq = get_ast_dlq()
            assert len(dlq) == 3
            assert dlq[0]["index"] == 2
            assert dlq[2]["index"] == 4
        finally:
            gb._AST_DLQ = original


class TestBackwardCompatLocalMode:

    @pytest.mark.asyncio
    async def test_use_remote_ast_false_uses_local_pool(self) -> None:
        from orchestrator.app.graph_builder import parse_source_ast

        state = {
            "raw_files": [
                {"path": "services/auth/main.go", "content": "package main"},
            ],
            "extracted_nodes": [],
            "extraction_checkpoint": {},
            "tenant_id": "test-tenant",
        }

        pool_was_used = []

        def _tracking_pool():
            pool_was_used.append(True)
            return MagicMock()

        async def _fake_run_in_executor(pool, fn, *args):
            return fn(*args)

        mock_loop = MagicMock()
        mock_loop.run_in_executor = _fake_run_in_executor

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder._USE_REMOTE_AST",
                False,
            ),
            patch(
                "orchestrator.app.graph_builder._get_process_pool",
                _tracking_pool,
            ),
            patch(
                "asyncio.get_running_loop",
                return_value=mock_loop,
            ),
        ):
            result = await parse_source_ast(state)

        assert pool_was_used, (
            "When USE_REMOTE_AST=false, ProcessPoolExecutor should still be used"
        )
        assert "extracted_nodes" in result
