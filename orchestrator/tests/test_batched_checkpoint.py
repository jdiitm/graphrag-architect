from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.checkpoint_store import CheckpointStoreConfig


class TestCheckpointStoreConfig:

    def test_batch_max_size_default(self) -> None:
        config = CheckpointStoreConfig()
        assert config.batch_max_size == 50

    def test_batch_flush_interval_ms_default(self) -> None:
        config = CheckpointStoreConfig()
        assert config.batch_flush_interval_ms == 500

    def test_config_from_env(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "CHECKPOINT_BACKEND": "postgres",
                "CHECKPOINT_POSTGRES_DSN": "postgresql://test",
                "CHECKPOINT_BATCH_MAX_SIZE": "100",
                "CHECKPOINT_BATCH_FLUSH_MS": "250",
            },
            clear=False,
        ):
            config = CheckpointStoreConfig.from_env()
            assert config.batch_max_size == 100
            assert config.batch_flush_interval_ms == 250


class TestBatchedCheckpointSaver:

    @pytest.mark.asyncio
    async def test_put_buffers_without_immediate_write(self) -> None:
        from orchestrator.app.batched_checkpoint import (
            BatchedCheckpointSaver,
        )
        inner = AsyncMock()
        saver = BatchedCheckpointSaver(
            inner_saver=inner,
            batch_max_size=10,
            flush_interval_ms=10000,
        )
        config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}

        await saver.aput(config, {"id": "cp-1"}, {"step": 1})

        assert saver.pending_count == 1
        inner.aput.assert_not_awaited()
        await saver.close()

    @pytest.mark.asyncio
    async def test_flush_triggered_at_batch_max(self) -> None:
        from orchestrator.app.batched_checkpoint import (
            BatchedCheckpointSaver,
        )
        inner = AsyncMock()
        saver = BatchedCheckpointSaver(
            inner_saver=inner,
            batch_max_size=2,
            flush_interval_ms=60000,
        )
        config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}

        await saver.aput(config, {"id": "cp-1"}, {"step": 1})
        assert saver.pending_count == 1

        await saver.aput(config, {"id": "cp-2"}, {"step": 2})
        assert saver.pending_count == 0
        assert inner.aput.await_count == 2
        await saver.close()

    @pytest.mark.asyncio
    async def test_flush_on_close(self) -> None:
        from orchestrator.app.batched_checkpoint import (
            BatchedCheckpointSaver,
        )
        inner = AsyncMock()
        saver = BatchedCheckpointSaver(
            inner_saver=inner,
            batch_max_size=100,
            flush_interval_ms=60000,
        )
        config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}

        await saver.aput(config, {"id": "cp-1"}, {"step": 1})
        await saver.aput(config, {"id": "cp-2"}, {"step": 2})

        assert saver.pending_count == 2
        await saver.close()
        assert saver.pending_count == 0
        assert inner.aput.await_count == 2

    @pytest.mark.asyncio
    async def test_aget_delegates_to_inner(self) -> None:
        from orchestrator.app.batched_checkpoint import (
            BatchedCheckpointSaver,
        )
        inner = AsyncMock()
        inner.aget_tuple = AsyncMock(return_value=("checkpoint", "metadata"))
        saver = BatchedCheckpointSaver(
            inner_saver=inner,
            batch_max_size=10,
            flush_interval_ms=60000,
        )
        config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}

        result = await saver.aget_tuple(config)
        assert result == ("checkpoint", "metadata")
        inner.aget_tuple.assert_awaited_once_with(config)
        await saver.close()

    @pytest.mark.asyncio
    async def test_concurrent_puts_are_safe(self) -> None:
        from orchestrator.app.batched_checkpoint import (
            BatchedCheckpointSaver,
        )
        inner = AsyncMock()
        saver = BatchedCheckpointSaver(
            inner_saver=inner,
            batch_max_size=100,
            flush_interval_ms=60000,
        )
        config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}

        tasks = [
            saver.aput(config, {"id": f"cp-{i}"}, {"step": i})
            for i in range(20)
        ]
        await asyncio.gather(*tasks)

        assert saver.pending_count + inner.aput.await_count == 20
        await saver.close()
        assert inner.aput.await_count == 20

    @pytest.mark.asyncio
    async def test_put_after_close_raises(self) -> None:
        from orchestrator.app.batched_checkpoint import (
            BatchedCheckpointSaver,
        )
        inner = AsyncMock()
        saver = BatchedCheckpointSaver(
            inner_saver=inner,
            batch_max_size=10,
            flush_interval_ms=60000,
        )
        await saver.close()

        with pytest.raises(RuntimeError, match="closed"):
            await saver.aput(
                {"configurable": {"thread_id": "t1"}},
                {"id": "cp-1"},
                {"step": 1},
            )

    @pytest.mark.asyncio
    async def test_flush_error_preserves_buffer(self) -> None:
        from orchestrator.app.batched_checkpoint import (
            BatchedCheckpointSaver,
        )
        call_count = 0

        async def _failing_put(config: dict, cp: dict, meta: dict) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise ConnectionError("db gone")

        inner = AsyncMock()
        inner.aput = _failing_put
        saver = BatchedCheckpointSaver(
            inner_saver=inner,
            batch_max_size=100,
            flush_interval_ms=60000,
        )
        config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}

        await saver.aput(config, {"id": "cp-1"}, {"step": 1})
        await saver.aput(config, {"id": "cp-2"}, {"step": 2})
        await saver.aput(config, {"id": "cp-3"}, {"step": 3})

        with pytest.raises(ConnectionError):
            await saver.flush()

        assert saver.pending_count == 2
        saver._closed = True

    @pytest.mark.asyncio
    async def test_timer_flushes_buffer_via_event(self) -> None:
        from orchestrator.app.batched_checkpoint import (
            BatchedCheckpointSaver,
        )
        flushed = asyncio.Event()
        original_flush = None

        async def _signaling_flush() -> None:
            await original_flush()
            flushed.set()

        inner = AsyncMock()
        saver = BatchedCheckpointSaver(
            inner_saver=inner,
            batch_max_size=1000,
            flush_interval_ms=10,
        )
        original_flush = saver.flush
        saver.flush = _signaling_flush

        config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
        await saver.aput(config, {"id": "cp-1"}, {"step": 1})
        assert saver.pending_count == 1

        await saver.start_timer()

        await asyncio.wait_for(flushed.wait(), timeout=2.0)

        assert saver.pending_count == 0
        assert inner.aput.await_count == 1
        await saver.close()

    @pytest.mark.asyncio
    async def test_timer_logs_on_flush_failure(self) -> None:
        from orchestrator.app.batched_checkpoint import (
            BatchedCheckpointSaver,
        )
        flush_attempted = asyncio.Event()

        async def _failing_flush() -> None:
            flush_attempted.set()
            raise ConnectionError("db gone")

        inner = AsyncMock()
        saver = BatchedCheckpointSaver(
            inner_saver=inner,
            batch_max_size=1000,
            flush_interval_ms=10,
        )
        saver.flush = _failing_flush

        config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
        await saver.aput(config, {"id": "cp-1"}, {"step": 1})

        with patch(
            "orchestrator.app.batched_checkpoint.logger"
        ) as mock_logger:
            await saver.start_timer()
            await asyncio.wait_for(flush_attempted.wait(), timeout=2.0)
            await asyncio.sleep(0.01)
            await saver.close()
            mock_logger.exception.assert_called()
            log_msg = mock_logger.exception.call_args[0][0]
            assert "flush failed" in log_msg.lower()

    @pytest.mark.asyncio
    async def test_start_timer_idempotent(self) -> None:
        from orchestrator.app.batched_checkpoint import (
            BatchedCheckpointSaver,
        )
        inner = AsyncMock()
        saver = BatchedCheckpointSaver(
            inner_saver=inner,
            batch_max_size=100,
            flush_interval_ms=60000,
        )
        await saver.start_timer()
        first_task = saver._timer_task
        await saver.start_timer()
        assert saver._timer_task is first_task
        await saver.close()


class TestCheckpointStoreIntegration:

    @pytest.mark.asyncio
    async def test_init_checkpointer_memory_backend_uses_plain_saver(
        self,
    ) -> None:
        from orchestrator.app.checkpoint_store import (
            _state,
            close_checkpointer,
            init_checkpointer,
        )
        with patch.dict(
            "os.environ",
            {"CHECKPOINT_BACKEND": "memory"},
            clear=False,
        ):
            await init_checkpointer()
            cp = _state["checkpointer"]
            from langgraph.checkpoint.memory import MemorySaver
            assert isinstance(cp, MemorySaver)
            await close_checkpointer()

    @pytest.mark.asyncio
    async def test_close_checkpointer_exception_safe(self) -> None:
        from orchestrator.app.checkpoint_store import (
            _state,
            close_checkpointer,
        )
        mock_cp = AsyncMock()
        mock_cp.close = AsyncMock(
            side_effect=ConnectionError("flush failed")
        )
        mock_conn = AsyncMock()

        _state["checkpointer"] = mock_cp
        _state["connection"] = mock_conn

        with pytest.raises(ConnectionError):
            await close_checkpointer()

        mock_conn.close.assert_awaited_once()
        assert _state["connection"] is None
        assert _state["checkpointer"] is None
