import asyncio
from contextlib import ExitStack
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.graph_builder import (
    _COALESCING_OUTBOX,
    _VECTOR_OUTBOX,
    _ASTPoolHolder,
    drain_vector_outbox_sync,
    flush_coalescing_outbox,
    shutdown_ast_pool,
)
from orchestrator.app.vector_sync_outbox import VectorSyncEvent


class TestShutdownAstPool:
    def test_shuts_down_existing_executor(self) -> None:
        mock_pool = MagicMock()
        original = _ASTPoolHolder.instance
        try:
            _ASTPoolHolder.instance = mock_pool
            shutdown_ast_pool()
            mock_pool.shutdown.assert_called_once_with(wait=False)
            assert _ASTPoolHolder.instance is None
        finally:
            _ASTPoolHolder.instance = original

    def test_noop_when_no_pool_exists(self) -> None:
        original = _ASTPoolHolder.instance
        try:
            _ASTPoolHolder.instance = None
            shutdown_ast_pool()
            assert _ASTPoolHolder.instance is None
        finally:
            _ASTPoolHolder.instance = original


class TestFlushCoalescingOutbox:
    def test_flushes_and_enqueues_to_vector_outbox(self) -> None:
        event = VectorSyncEvent(collection="test_col", pruned_ids=["id1"])
        with patch.object(
            _COALESCING_OUTBOX, "flush", return_value=[event],
        ), patch.object(
            _VECTOR_OUTBOX, "enqueue",
        ) as mock_enqueue:
            result = flush_coalescing_outbox()
        assert result == 1
        mock_enqueue.assert_called_once_with(event)

    def test_returns_zero_when_empty(self) -> None:
        with patch.object(_COALESCING_OUTBOX, "flush", return_value=[]):
            result = flush_coalescing_outbox()
        assert result == 0


class TestDrainVectorOutboxSync:
    def test_returns_count_of_pending_events(self) -> None:
        events = [
            VectorSyncEvent(collection="col_a", pruned_ids=["x"]),
            VectorSyncEvent(collection="col_b", pruned_ids=["y"]),
        ]
        with patch.object(_VECTOR_OUTBOX, "drain_pending", return_value=events):
            count = drain_vector_outbox_sync()
        assert count == 2

    def test_returns_zero_when_empty(self) -> None:
        with patch.object(_VECTOR_OUTBOX, "drain_pending", return_value=[]):
            count = drain_vector_outbox_sync()
        assert count == 0


def _build_lifespan_patches() -> dict[str, Any]:
    bg_tasks = MagicMock()
    bg_tasks.drain_all = AsyncMock(return_value=0)
    validator_mock = MagicMock()
    rate_limit_mock = MagicMock()
    rate_limit_mock.from_env.return_value.max_concurrent_ingestions = 5
    return {
        "orchestrator.app.main.ProductionConfigValidator": validator_mock,
        "orchestrator.app.main.TenantScopeVerifier": MagicMock(),
        "orchestrator.app.main.TemplateCatalog": MagicMock(),
        "orchestrator.app.main._validate_startup_security": MagicMock(
            return_value=MagicMock(),
        ),
        "orchestrator.app.main.configure_telemetry": MagicMock(),
        "orchestrator.app.main.configure_metrics": MagicMock(),
        "orchestrator.app.main.init_checkpointer": AsyncMock(),
        "orchestrator.app.main.initialize_ingestion_graph": MagicMock(),
        "orchestrator.app.main.init_driver": MagicMock(),
        "orchestrator.app.main.create_ingestion_semaphore": MagicMock(),
        "orchestrator.app.main.RateLimitConfig": rate_limit_mock,
        "orchestrator.app.main.set_ingestion_semaphore": MagicMock(),
        "orchestrator.app.main._warn_insecure_auth": MagicMock(),
        "orchestrator.app.main._kafka_consumer_enabled": MagicMock(
            return_value=False,
        ),
        "orchestrator.app.main.shutdown_pool": MagicMock(),
        "orchestrator.app.main.shutdown_thread_pool": MagicMock(),
        "orchestrator.app.main.close_driver": AsyncMock(),
        "orchestrator.app.main.close_checkpointer": AsyncMock(),
        "orchestrator.app.graph_builder._BACKGROUND_TASKS": bg_tasks,
    }


class TestLifespanDrainsActiveIngestTasks:
    @pytest.mark.asyncio
    async def test_active_ingest_tasks_cancelled_on_shutdown(self) -> None:
        from orchestrator.app.main import _ACTIVE_INGEST_TASKS, app, lifespan

        patches = _build_lifespan_patches()

        async def _stall() -> None:
            await asyncio.sleep(3600)

        with ExitStack() as stack:
            for target, obj in patches.items():
                stack.enter_context(patch(target, obj))
            stack.enter_context(
                patch(
                    "orchestrator.app.main.flush_coalescing_outbox",
                    return_value=0,
                ),
            )
            stack.enter_context(
                patch(
                    "orchestrator.app.main.drain_vector_outbox_sync",
                    return_value=0,
                ),
            )
            stack.enter_context(
                patch("orchestrator.app.main.shutdown_ast_pool"),
            )

            task = asyncio.create_task(_stall())
            _ACTIVE_INGEST_TASKS.add(task)
            try:
                async with lifespan(app):
                    pass
            finally:
                _ACTIVE_INGEST_TASKS.discard(task)

            assert task.cancelled()


class TestLifespanDrainsActiveQueryTasks:
    @pytest.mark.asyncio
    async def test_active_query_tasks_cancelled_on_shutdown(self) -> None:
        from orchestrator.app.main import _ACTIVE_QUERY_TASKS, app, lifespan

        patches = _build_lifespan_patches()

        async def _stall() -> None:
            await asyncio.sleep(3600)

        with ExitStack() as stack:
            for target, obj in patches.items():
                stack.enter_context(patch(target, obj))
            stack.enter_context(
                patch(
                    "orchestrator.app.main.flush_coalescing_outbox",
                    return_value=0,
                ),
            )
            stack.enter_context(
                patch(
                    "orchestrator.app.main.drain_vector_outbox_sync",
                    return_value=0,
                ),
            )
            stack.enter_context(
                patch("orchestrator.app.main.shutdown_ast_pool"),
            )

            task = asyncio.create_task(_stall())
            _ACTIVE_QUERY_TASKS.add(task)
            try:
                async with lifespan(app):
                    pass
            finally:
                _ACTIVE_QUERY_TASKS.discard(task)

            assert task.cancelled()


class TestLifespanFlushesVectorOutbox:
    @pytest.mark.asyncio
    async def test_flush_and_drain_called_on_shutdown(self) -> None:
        from orchestrator.app.main import app, lifespan

        patches = _build_lifespan_patches()
        mock_flush = MagicMock(return_value=0)
        mock_drain = MagicMock(return_value=0)

        with ExitStack() as stack:
            for target, obj in patches.items():
                stack.enter_context(patch(target, obj))
            stack.enter_context(
                patch(
                    "orchestrator.app.main.flush_coalescing_outbox",
                    mock_flush,
                ),
            )
            stack.enter_context(
                patch(
                    "orchestrator.app.main.drain_vector_outbox_sync",
                    mock_drain,
                ),
            )
            stack.enter_context(
                patch("orchestrator.app.main.shutdown_ast_pool"),
            )

            async with lifespan(app):
                pass

            mock_flush.assert_called_once()
            mock_drain.assert_called_once()


class TestLifespanShutsDownAstPool:
    @pytest.mark.asyncio
    async def test_ast_pool_shutdown_called(self) -> None:
        from orchestrator.app.main import app, lifespan

        patches = _build_lifespan_patches()
        mock_ast_shutdown = MagicMock()

        with ExitStack() as stack:
            for target, obj in patches.items():
                stack.enter_context(patch(target, obj))
            stack.enter_context(
                patch(
                    "orchestrator.app.main.flush_coalescing_outbox",
                    return_value=0,
                ),
            )
            stack.enter_context(
                patch(
                    "orchestrator.app.main.drain_vector_outbox_sync",
                    return_value=0,
                ),
            )
            stack.enter_context(
                patch(
                    "orchestrator.app.main.shutdown_ast_pool",
                    mock_ast_shutdown,
                ),
            )

            async with lifespan(app):
                pass

            mock_ast_shutdown.assert_called_once()
