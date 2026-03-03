from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.distributed_lock import DistributedLock


class TestExponentialBackoff:

    @pytest.mark.asyncio
    async def test_retry_delays_increase_exponentially(self) -> None:
        redis_conn = AsyncMock()
        redis_conn.set = AsyncMock(return_value=None)
        redis_conn.delete = AsyncMock()

        lock = DistributedLock(redis_conn, retry_attempts=5, retry_delay=0.1)

        sleep_calls: list[float] = []
        original_sleep = __import__("asyncio").sleep

        async def _capture_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        with patch("orchestrator.app.distributed_lock.asyncio.sleep", side_effect=_capture_sleep):
            with pytest.raises(TimeoutError):
                async with lock.acquire("test-key"):
                    pass

        assert len(sleep_calls) == 5
        for i in range(1, len(sleep_calls)):
            assert sleep_calls[i] > sleep_calls[i - 1], (
                f"Delay at attempt {i} ({sleep_calls[i]:.4f}) must be greater "
                f"than at attempt {i - 1} ({sleep_calls[i - 1]:.4f})"
            )

    @pytest.mark.asyncio
    async def test_delays_have_jitter(self) -> None:
        redis_conn = AsyncMock()
        redis_conn.set = AsyncMock(return_value=None)
        redis_conn.delete = AsyncMock()

        lock = DistributedLock(redis_conn, retry_attempts=5, retry_delay=0.1)

        all_delays: list[list[float]] = []
        for _ in range(10):
            sleep_calls: list[float] = []

            async def _capture(delay: float) -> None:
                sleep_calls.append(delay)

            with patch("orchestrator.app.distributed_lock.asyncio.sleep", side_effect=_capture):
                with pytest.raises(TimeoutError):
                    async with lock.acquire("test-key"):
                        pass
            all_delays.append(list(sleep_calls))

        per_attempt_values = list(zip(*all_delays))
        has_jitter = False
        for attempt_delays in per_attempt_values:
            if len(set(round(d, 6) for d in attempt_delays)) > 1:
                has_jitter = True
                break
        assert has_jitter, "Retry delays must include randomized jitter"

    @pytest.mark.asyncio
    async def test_total_retry_time_is_bounded(self) -> None:
        redis_conn = AsyncMock()
        redis_conn.set = AsyncMock(return_value=None)
        redis_conn.delete = AsyncMock()

        lock = DistributedLock(redis_conn, retry_attempts=10, retry_delay=0.1)

        sleep_calls: list[float] = []

        async def _capture(delay: float) -> None:
            sleep_calls.append(delay)

        with patch("orchestrator.app.distributed_lock.asyncio.sleep", side_effect=_capture):
            with pytest.raises(TimeoutError):
                async with lock.acquire("test-key"):
                    pass

        total = sum(sleep_calls)
        assert total < 30.0, (
            f"Total retry time {total:.2f}s must be bounded under 30s"
        )

    @pytest.mark.asyncio
    async def test_max_delay_is_capped(self) -> None:
        redis_conn = AsyncMock()
        redis_conn.set = AsyncMock(return_value=None)
        redis_conn.delete = AsyncMock()

        lock = DistributedLock(redis_conn, retry_attempts=20, retry_delay=0.1)

        sleep_calls: list[float] = []

        async def _capture(delay: float) -> None:
            sleep_calls.append(delay)

        with patch("orchestrator.app.distributed_lock.asyncio.sleep", side_effect=_capture):
            with pytest.raises(TimeoutError):
                async with lock.acquire("test-key"):
                    pass

        for delay in sleep_calls:
            assert delay <= 3.0, (
                f"Individual delay {delay:.3f}s must be capped"
            )
