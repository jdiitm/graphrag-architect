from __future__ import annotations

import asyncio
import time


class AdaptiveTokenBucket:
    def __init__(
        self,
        capacity: int = 10,
        refill_rate: float = 10.0,
        min_rate: float = 1.0,
        max_rate: float = 100.0,
        additive_increase: float = 1.0,
    ) -> None:
        self._capacity = capacity
        self._tokens = float(capacity)
        self._refill_rate = refill_rate
        self._min_rate = min_rate
        self._max_rate = max_rate
        self._additive_increase = additive_increase
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    @property
    def current_rate(self) -> float:
        return self._refill_rate

    @property
    def available_tokens(self) -> int:
        return int(self._tokens)

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            self._capacity, self._tokens + elapsed * self._refill_rate
        )
        self._last_refill = now

    async def acquire(self) -> bool:
        async with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            wait_time = (1.0 - self._tokens) / self._refill_rate
        await asyncio.sleep(wait_time)
        async with self._lock:
            self._refill()
            self._tokens -= 1.0
            return True

    def record_throttle(self) -> None:
        self._refill_rate = max(
            self._min_rate, self._refill_rate / 2
        )

    def record_success(self) -> None:
        self._refill_rate = min(
            self._max_rate, self._refill_rate + self._additive_increase
        )
