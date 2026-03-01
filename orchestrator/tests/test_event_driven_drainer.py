from __future__ import annotations

import asyncio

import pytest

from orchestrator.app.graph_builder import PeriodicVectorDrainer


class TestEventDrivenDrainer:

    @pytest.mark.asyncio
    async def test_drainer_supports_notify(self) -> None:
        assert hasattr(PeriodicVectorDrainer, "notify"), (
            "PeriodicVectorDrainer must support notify() for event-driven "
            "drain triggering instead of relying solely on sleep polling"
        )

    @pytest.mark.asyncio
    async def test_notify_triggers_immediate_drain(self) -> None:
        drain_count = 0

        async def counting_drain() -> None:
            nonlocal drain_count
            drain_count += 1

        drainer = PeriodicVectorDrainer(
            drain_fn=counting_drain,
            interval_seconds=60.0,
        )
        task = drainer.start()

        await asyncio.sleep(0.05)
        drainer.notify()
        await asyncio.sleep(0.15)
        drainer.stop()

        try:
            await asyncio.wait_for(task, timeout=1.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

        assert drain_count >= 2, (
            f"notify() should trigger an immediate drain beyond the initial "
            f"run. Got {drain_count} drains in 0.2s with 60s interval."
        )

    @pytest.mark.asyncio
    async def test_polling_still_works_without_notify(self) -> None:
        drain_count = 0

        async def counting_drain() -> None:
            nonlocal drain_count
            drain_count += 1

        drainer = PeriodicVectorDrainer(
            drain_fn=counting_drain,
            interval_seconds=0.1,
        )
        task = drainer.start()
        await asyncio.sleep(0.35)
        drainer.stop()

        try:
            await asyncio.wait_for(task, timeout=1.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

        assert drain_count >= 2
