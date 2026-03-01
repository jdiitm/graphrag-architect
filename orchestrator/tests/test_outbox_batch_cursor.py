from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from orchestrator.app.vector_sync_outbox import (
    CoalescingOutbox,
    VectorSyncEvent,
)


def _make_upsert_event(node_id: str) -> VectorSyncEvent:
    vec = MagicMock()
    vec.id = node_id
    return VectorSyncEvent(
        collection="default",
        operation="upsert",
        vectors=[vec],
    )


class TestOutboxBatchCursor:

    def test_outbox_supports_batch_size_parameter(self) -> None:
        outbox = CoalescingOutbox(window_seconds=0)
        assert hasattr(outbox, "drain_batch"), (
            "CoalescingOutbox must support drain_batch() for cursor-based "
            "pagination instead of draining all pending events at once"
        )

    def test_drain_batch_returns_limited_events(self) -> None:
        outbox = CoalescingOutbox(window_seconds=0)
        for i in range(10):
            outbox.enqueue(_make_upsert_event(f"node-{i}"))

        batch = outbox.drain_batch(batch_size=3)
        assert len(batch) <= 3
        assert len(batch) > 0

    def test_drain_batch_returns_remaining_on_subsequent_calls(
        self,
    ) -> None:
        outbox = CoalescingOutbox(window_seconds=0)
        for i in range(5):
            outbox.enqueue(_make_upsert_event(f"node-{i}"))

        first = outbox.drain_batch(batch_size=2)
        second = outbox.drain_batch(batch_size=2)
        third = outbox.drain_batch(batch_size=2)
        total = len(first) + len(second) + len(third)
        assert total == 5

    def test_drain_batch_empty_returns_empty(self) -> None:
        outbox = CoalescingOutbox(window_seconds=0)
        batch = outbox.drain_batch(batch_size=10)
        assert batch == []
