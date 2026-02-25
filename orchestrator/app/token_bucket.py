from __future__ import annotations

import asyncio
import time
from typing import Dict


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

    async def try_acquire(self) -> bool:
        async with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


class TenantRateLimiter:
    def __init__(
        self,
        capacity: int = 10,
        refill_rate: float = 10.0,
        max_tenants: int = 10_000,
    ) -> None:
        self._capacity = capacity
        self._refill_rate = refill_rate
        self._max_tenants = max_tenants
        self._buckets: Dict[str, AdaptiveTokenBucket] = {}
        self._access_order: Dict[str, float] = {}

    def _evict_oldest(self) -> None:
        if len(self._buckets) < self._max_tenants:
            return
        oldest_key = min(self._access_order, key=self._access_order.get)
        del self._buckets[oldest_key]
        del self._access_order[oldest_key]

    def _get_bucket(self, tenant_id: str) -> AdaptiveTokenBucket:
        if tenant_id not in self._buckets:
            self._evict_oldest()
            self._buckets[tenant_id] = AdaptiveTokenBucket(
                capacity=self._capacity,
                refill_rate=self._refill_rate,
            )
        self._access_order[tenant_id] = time.monotonic()
        return self._buckets[tenant_id]

    async def try_acquire(self, tenant_id: str) -> bool:
        bucket = self._get_bucket(tenant_id)
        return await bucket.try_acquire()

    @property
    def active_tenants(self) -> int:
        return len(self._buckets)
