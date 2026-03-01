from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from orchestrator.app.query_models import QueryComplexity
from orchestrator.app.redis_client import create_async_redis, require_redis

logger = logging.getLogger(__name__)


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


class RedisRateLimiter:
    def __init__(
        self,
        redis_url: str,
        capacity: int = 20,
        window_seconds: int = 60,
        key_prefix: str = "graphrag:ratelimit:",
        password: str = "",
        db: int = 0,
    ) -> None:
        require_redis("RedisRateLimiter")
        self._redis = create_async_redis(redis_url, password=password, db=db)
        self._capacity = capacity
        self._window = window_seconds
        self._prefix = key_prefix

    async def try_acquire(self, tenant_id: str) -> bool:
        key = f"{self._prefix}{tenant_id}"
        now = time.time()
        window_start = now - self._window
        member = f"{now}:{uuid.uuid4().hex[:8]}"
        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.zremrangebyscore(key, "-inf", window_start)
                pipe.zadd(key, {member: now})
                pipe.expire(key, self._window + 1)
                pipe.zcard(key)
                results = await pipe.execute()
            count = results[3]
            if count > self._capacity:
                await self._redis.zrem(key, member)
                return False
            return True
        except Exception:
            logger.warning("Redis rate limiter failed, allowing request")
            return True


@dataclass(frozen=True)
class QueryCostModel:
    entity_lookup: int = 1
    single_hop: int = 3
    multi_hop: int = 10
    aggregate: int = 8

    def cost_for(self, complexity: QueryComplexity) -> int:
        mapping: Dict[QueryComplexity, int] = {
            QueryComplexity.ENTITY_LOOKUP: self.entity_lookup,
            QueryComplexity.SINGLE_HOP: self.single_hop,
            QueryComplexity.MULTI_HOP: self.multi_hop,
            QueryComplexity.AGGREGATE: self.aggregate,
        }
        return mapping.get(complexity, 1)

    @classmethod
    def from_env(cls) -> QueryCostModel:
        return cls(
            entity_lookup=int(os.environ.get("QUERY_COST_ENTITY_LOOKUP", "1")),
            single_hop=int(os.environ.get("QUERY_COST_SINGLE_HOP", "3")),
            multi_hop=int(os.environ.get("QUERY_COST_MULTI_HOP", "10")),
            aggregate=int(os.environ.get("QUERY_COST_AGGREGATE", "8")),
        )


class TenantCostBudget:
    def __init__(
        self,
        capacity: int = 100,
        window_seconds: float = 60.0,
        cost_model: Optional[QueryCostModel] = None,
    ) -> None:
        self._capacity = capacity
        self._window = window_seconds
        self._cost_model = cost_model or QueryCostModel()
        self._usage: Dict[str, List[Tuple[float, int]]] = {}
        self._lock = asyncio.Lock()

    async def try_acquire(self, tenant_id: str, complexity: QueryComplexity) -> bool:
        cost = self._cost_model.cost_for(complexity)
        async with self._lock:
            now = time.monotonic()
            entries = self._usage.get(tenant_id, [])
            entries = [(t, c) for t, c in entries if now - t < self._window]
            current_usage = sum(c for _, c in entries)
            if current_usage + cost > self._capacity:
                self._usage[tenant_id] = entries
                return False
            entries.append((now, cost))
            self._usage[tenant_id] = entries
            return True


def create_cost_budget(
    capacity: int = 100,
    window_seconds: float = 60.0,
) -> TenantCostBudget:
    cost_model = QueryCostModel.from_env()
    return TenantCostBudget(
        capacity=capacity,
        window_seconds=window_seconds,
        cost_model=cost_model,
    )


def create_rate_limiter(
    capacity: int = 20,
    refill_rate: float = 10.0,
) -> Any:
    from orchestrator.app.config import RedisConfig
    redis_cfg = RedisConfig.from_env()
    if redis_cfg.url:
        return RedisRateLimiter(
            redis_url=redis_cfg.url,
            password=redis_cfg.password,
            db=redis_cfg.db,
            capacity=capacity,
        )
    return TenantRateLimiter(capacity=capacity, refill_rate=refill_rate)
