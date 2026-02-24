from __future__ import annotations

import asyncio
import time

import pytest

from orchestrator.app.token_bucket import AdaptiveTokenBucket


class TestAdaptiveTokenBucket:
    def test_acquire_succeeds_when_tokens_available(self) -> None:
        bucket = AdaptiveTokenBucket(capacity=5, refill_rate=10.0)

        async def _run():
            return await bucket.acquire()

        assert asyncio.run(_run()) is True

    def test_acquire_blocks_when_empty(self) -> None:
        bucket = AdaptiveTokenBucket(capacity=1, refill_rate=100.0)

        async def _run():
            await bucket.acquire()
            start = time.monotonic()
            await bucket.acquire()
            return time.monotonic() - start

        elapsed = asyncio.run(_run())
        assert elapsed < 0.1

    def test_capacity_limits_burst(self) -> None:
        bucket = AdaptiveTokenBucket(capacity=3, refill_rate=0.1)

        async def _run():
            acquired = 0
            for _ in range(5):
                got = await asyncio.wait_for(bucket.acquire(), timeout=0.05)
                if got:
                    acquired += 1
            return acquired

        with pytest.raises(asyncio.TimeoutError):
            asyncio.run(_run())

    def test_record_throttle_halves_rate(self) -> None:
        bucket = AdaptiveTokenBucket(
            capacity=10, refill_rate=20.0, min_rate=1.0,
        )
        initial_rate = bucket.current_rate
        bucket.record_throttle()
        assert bucket.current_rate == initial_rate / 2

    def test_record_success_increments_rate(self) -> None:
        bucket = AdaptiveTokenBucket(
            capacity=10, refill_rate=10.0, additive_increase=2.0,
        )
        initial_rate = bucket.current_rate
        bucket.record_success()
        assert bucket.current_rate == initial_rate + 2.0

    def test_rate_does_not_exceed_max(self) -> None:
        bucket = AdaptiveTokenBucket(
            capacity=10, refill_rate=10.0, max_rate=12.0,
            additive_increase=5.0,
        )
        bucket.record_success()
        assert bucket.current_rate == 12.0

    def test_rate_does_not_go_below_min(self) -> None:
        bucket = AdaptiveTokenBucket(
            capacity=10, refill_rate=2.0, min_rate=1.0,
        )
        bucket.record_throttle()
        assert bucket.current_rate == 1.0

    def test_aimd_convergence(self) -> None:
        bucket = AdaptiveTokenBucket(
            capacity=10, refill_rate=10.0,
            min_rate=1.0, max_rate=20.0,
            additive_increase=1.0,
        )
        bucket.record_throttle()
        rate_after_decrease = bucket.current_rate
        for _ in range(5):
            bucket.record_success()
        rate_after_recovery = bucket.current_rate
        assert rate_after_recovery > rate_after_decrease

    def test_token_count_property(self) -> None:
        bucket = AdaptiveTokenBucket(capacity=5, refill_rate=10.0)
        assert bucket.available_tokens == 5

    def test_concurrent_acquires(self) -> None:
        bucket = AdaptiveTokenBucket(capacity=10, refill_rate=100.0)

        async def _run():
            tasks = [bucket.acquire() for _ in range(10)]
            results = await asyncio.gather(*tasks)
            return sum(1 for r in results if r)

        acquired = asyncio.run(_run())
        assert acquired == 10
