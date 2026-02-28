from __future__ import annotations

import asyncio
import time
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.distributed_lock import BoundedTaskSet
from orchestrator.app.vector_sync_outbox import (
    CoalescingOutbox,
    VectorSyncEvent,
)


class TestBoundedTaskSetOverflowCallback:

    @pytest.mark.asyncio
    async def test_overflow_callback_invoked_when_at_capacity(self) -> None:
        callback = MagicMock()
        bts = BoundedTaskSet(max_tasks=1, on_overflow=callback)

        async def _block() -> None:
            await asyncio.sleep(10)

        t1 = asyncio.create_task(_block())
        t2 = asyncio.create_task(_block())
        bts.try_add(t1)
        result = bts.try_add(t2)

        assert result is False
        callback.assert_called_once()
        t1.cancel()
        t2.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t1
        with pytest.raises(asyncio.CancelledError):
            await t2

    @pytest.mark.asyncio
    async def test_overflow_callback_not_invoked_under_capacity(self) -> None:
        callback = MagicMock()
        bts = BoundedTaskSet(max_tasks=5, on_overflow=callback)

        async def _fast() -> None:
            pass

        t1 = asyncio.create_task(_fast())
        bts.try_add(t1)

        callback.assert_not_called()
        await t1

    @pytest.mark.asyncio
    async def test_overflow_counter_tracks_rejections(self) -> None:
        bts = BoundedTaskSet(max_tasks=1)

        async def _block() -> None:
            await asyncio.sleep(10)

        t1 = asyncio.create_task(_block())
        bts.try_add(t1)

        rejected_tasks = []
        for _ in range(3):
            t = asyncio.create_task(_block())
            bts.try_add(t)
            rejected_tasks.append(t)

        assert bts.overflow_count == 3

        t1.cancel()
        for t in rejected_tasks:
            t.cancel()
        await asyncio.sleep(0)

    @pytest.mark.asyncio
    async def test_no_overflow_callback_preserves_original_behavior(self) -> None:
        bts = BoundedTaskSet(max_tasks=1)

        async def _block() -> None:
            await asyncio.sleep(10)

        t1 = asyncio.create_task(_block())
        t2 = asyncio.create_task(_block())
        bts.try_add(t1)
        result = bts.try_add(t2)

        assert result is False
        await asyncio.sleep(0)
        assert t2.cancelled()
        t1.cancel()
        await asyncio.sleep(0)


class TestCoalescingOutboxMemoryCap:

    def test_enqueue_within_cap_does_not_spill(self) -> None:
        spilled: List[VectorSyncEvent] = []
        outbox = CoalescingOutbox(
            window_seconds=0.0,
            max_entries=5,
            spillover_fn=spilled.extend,
        )
        for i in range(5):
            outbox.enqueue(VectorSyncEvent(
                collection="svc", pruned_ids=[f"node-{i}"],
            ))

        assert outbox.pending_count == 5
        assert len(spilled) == 0

    def test_enqueue_beyond_cap_triggers_spillover(self) -> None:
        spilled: List[VectorSyncEvent] = []
        outbox = CoalescingOutbox(
            window_seconds=10.0,
            max_entries=3,
            spillover_fn=spilled.extend,
        )
        for i in range(5):
            outbox.enqueue(VectorSyncEvent(
                collection="svc", pruned_ids=[f"node-{i}"],
            ))

        assert outbox.pending_count <= 3
        assert len(spilled) >= 2

    def test_spilled_events_are_oldest_entries(self) -> None:
        spilled: List[VectorSyncEvent] = []
        outbox = CoalescingOutbox(
            window_seconds=10.0,
            max_entries=2,
            spillover_fn=spilled.extend,
        )
        outbox.enqueue(VectorSyncEvent(
            collection="svc", pruned_ids=["oldest"],
        ))
        outbox.enqueue(VectorSyncEvent(
            collection="svc", pruned_ids=["middle"],
        ))
        outbox.enqueue(VectorSyncEvent(
            collection="svc", pruned_ids=["newest"],
        ))

        spilled_ids = [e.pruned_ids[0] for e in spilled]
        assert "oldest" in spilled_ids
        assert outbox.pending_count <= 2

    def test_no_spillover_fn_preserves_original_behavior(self) -> None:
        outbox = CoalescingOutbox(window_seconds=0.0)
        for i in range(100):
            outbox.enqueue(VectorSyncEvent(
                collection="svc", pruned_ids=[f"node-{i}"],
            ))
        assert outbox.pending_count == 100

    def test_coalescing_still_works_with_cap(self) -> None:
        spilled: List[VectorSyncEvent] = []
        outbox = CoalescingOutbox(
            window_seconds=0.0,
            max_entries=5,
            spillover_fn=spilled.extend,
        )
        for _ in range(50):
            outbox.enqueue(VectorSyncEvent(
                collection="svc", pruned_ids=["same-node"],
            ))

        assert outbox.pending_count == 1
        assert len(spilled) == 0


class TestPeriodicDrainer:

    @pytest.mark.asyncio
    async def test_periodic_drain_processes_accumulated_events(self) -> None:
        from orchestrator.app.graph_builder import PeriodicVectorDrainer

        drain_mock = AsyncMock(return_value=3)
        drainer = PeriodicVectorDrainer(
            drain_fn=drain_mock,
            interval_seconds=0.05,
        )

        task = drainer.start()
        await asyncio.sleep(0.15)
        drainer.stop()
        await asyncio.sleep(0.05)

        assert drain_mock.await_count >= 2

    @pytest.mark.asyncio
    async def test_periodic_drainer_survives_drain_errors(self) -> None:
        from orchestrator.app.graph_builder import PeriodicVectorDrainer

        call_count = 0

        async def _flaky_drain() -> int:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("transient")
            return 1

        drainer = PeriodicVectorDrainer(
            drain_fn=_flaky_drain,
            interval_seconds=0.05,
        )
        task = drainer.start()
        await asyncio.sleep(0.2)
        drainer.stop()
        await asyncio.sleep(0.05)

        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_periodic_drainer_stop_is_idempotent(self) -> None:
        from orchestrator.app.graph_builder import PeriodicVectorDrainer

        drainer = PeriodicVectorDrainer(
            drain_fn=AsyncMock(return_value=0),
            interval_seconds=0.1,
        )
        task = drainer.start()
        drainer.stop()
        drainer.stop()
        await asyncio.sleep(0.05)

    @pytest.mark.asyncio
    async def test_periodic_drainer_is_not_subject_to_bounded_task_set(
        self,
    ) -> None:
        from orchestrator.app.graph_builder import PeriodicVectorDrainer

        bts = BoundedTaskSet(max_tasks=1)

        async def _block() -> None:
            await asyncio.sleep(10)

        blocker = asyncio.create_task(_block())
        bts.try_add(blocker)

        drain_count = 0

        async def _counting_drain() -> int:
            nonlocal drain_count
            drain_count += 1
            return 1

        drainer = PeriodicVectorDrainer(
            drain_fn=_counting_drain,
            interval_seconds=0.05,
        )
        task = drainer.start()
        await asyncio.sleep(0.15)
        drainer.stop()
        blocker.cancel()
        await asyncio.sleep(0.05)

        assert drain_count >= 2


class TestGraphBuilderOverflowIntegration:

    @pytest.mark.asyncio
    async def test_overflow_flushes_coalescing_outbox_to_durable_store(
        self,
    ) -> None:
        from orchestrator.app.graph_builder import (
            _BACKGROUND_TASKS,
            _COALESCING_OUTBOX,
            _VECTOR_OUTBOX,
            _post_commit_side_effects,
        )
        from orchestrator.app.vector_sync_outbox import (
            CoalescingOutbox,
            VectorSyncOutbox,
        )

        test_outbox = VectorSyncOutbox()
        test_coalescing = CoalescingOutbox(window_seconds=0.0)

        test_coalescing.enqueue(VectorSyncEvent(
            collection="svc", pruned_ids=["pre-existing-node"],
        ))

        mock_repo = AsyncMock()
        mock_repo.prune_stale_edges = AsyncMock(
            return_value=(1, ["stale-edge-1"]),
        )

        mock_span = MagicMock()
        mock_span.set_attribute = MagicMock()

        original_max = _BACKGROUND_TASKS._max
        _BACKGROUND_TASKS._max = 0

        try:
            with (
                patch(
                    "orchestrator.app.graph_builder._COALESCING_OUTBOX",
                    test_coalescing,
                ),
                patch(
                    "orchestrator.app.graph_builder._VECTOR_OUTBOX",
                    test_outbox,
                ),
                patch(
                    "orchestrator.app.graph_builder._coalescing_enabled",
                    return_value=True,
                ),
                patch(
                    "orchestrator.app.graph_builder.invalidate_caches_after_ingest",
                    new_callable=AsyncMock,
                ),
                patch(
                    "orchestrator.app.graph_builder.drain_vector_outbox",
                    new_callable=AsyncMock,
                ),
            ):
                await _post_commit_side_effects(
                    repo=mock_repo,
                    ingestion_id="test-ingest",
                    tenant_id="test-tenant",
                    span=mock_span,
                )
        finally:
            _BACKGROUND_TASKS._max = original_max

        assert test_outbox.pending_count >= 1, (
            "Events must be rescued to VectorSyncOutbox when BoundedTaskSet "
            "overflows, not silently lost"
        )
        assert test_coalescing.pending_count == 0, (
            "CoalescingOutbox must be flushed on overflow"
        )
