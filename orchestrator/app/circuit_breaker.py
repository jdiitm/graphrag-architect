from __future__ import annotations

import asyncio
import enum
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol, TypeVar, runtime_checkable

try:
    import redis.asyncio as aioredis
except ImportError:  # pragma: no cover
    aioredis = None  # type: ignore[assignment]

T = TypeVar("T")

_PROVIDER_RATE_LIMIT_PATTERNS = ("429", "rate limit", "resource_exhausted", "quota")


def _always_trip(_exc: Exception) -> bool:
    return True


def is_provider_rate_limit(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(p in text for p in _PROVIDER_RATE_LIMIT_PATTERNS)


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    pass


@dataclass(frozen=True)
class CircuitBreakerConfig:
    failure_threshold: int = 3
    recovery_timeout: float = 30.0
    half_open_max_calls: int = 1


@dataclass
class CircuitSnapshot:
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure_time: float = 0.0
    half_open_calls: int = 0


@runtime_checkable
class StateStore(Protocol):
    async def load(self, name: str) -> CircuitSnapshot: ...
    async def save(self, name: str, snapshot: CircuitSnapshot) -> None: ...


class InMemoryStateStore:
    def __init__(self) -> None:
        self._data: dict[str, CircuitSnapshot] = {}

    async def load(self, name: str) -> CircuitSnapshot:
        return self._data.get(name, CircuitSnapshot())

    async def save(self, name: str, snapshot: CircuitSnapshot) -> None:
        self._data[name] = snapshot


class RedisStateStore:
    def __init__(
        self,
        url: str,
        key_prefix: str = "graphrag:cb:",
        ttl_seconds: int = 300,
        password: str = "",
        db: int = 0,
    ) -> None:
        if aioredis is None:
            raise ImportError("redis package is required for RedisStateStore")
        kwargs: dict[str, Any] = {"decode_responses": True, "db": db}
        if password:
            kwargs["password"] = password
        self._redis = aioredis.from_url(url, **kwargs)
        self._prefix = key_prefix
        self._ttl = ttl_seconds

    def _key(self, name: str) -> str:
        return f"{self._prefix}{name}"

    async def load(self, name: str) -> CircuitSnapshot:
        data = await self._redis.hgetall(self._key(name))
        if not data:
            return CircuitSnapshot()
        return CircuitSnapshot(
            state=CircuitState(data.get("state", "closed")),
            failure_count=int(data.get("failure_count", 0)),
            last_failure_time=float(data.get("last_failure_time", 0.0)),
            half_open_calls=int(data.get("half_open_calls", 0)),
        )

    async def save(self, name: str, snapshot: CircuitSnapshot) -> None:
        key = self._key(name)
        mapping = {
            "state": snapshot.state.value,
            "failure_count": str(snapshot.failure_count),
            "last_failure_time": str(snapshot.last_failure_time),
            "half_open_calls": str(snapshot.half_open_calls),
        }
        pipe = self._redis.pipeline()
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, self._ttl)
        await pipe.execute()

    async def close(self) -> None:
        await self._redis.aclose()


class CircuitBreaker:
    def __init__(
        self,
        config: CircuitBreakerConfig,
        store: StateStore | None = None,
        name: str = "default",
        should_trip: Callable[[Exception], bool] | None = None,
    ) -> None:
        self._config = config
        self._store: StateStore = store or InMemoryStateStore()
        self._name = name
        self._lock: asyncio.Lock | None = None
        self._should_trip = should_trip or _always_trip

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    @property
    def state(self) -> CircuitState:
        return self._resolve_state_sync(
            CircuitSnapshot(
                state=self._sync_state,
                failure_count=self._sync_failure_count,
                last_failure_time=self._sync_last_failure_time,
                half_open_calls=self._sync_half_open_calls,
            )
        ).state

    def _resolve_state_sync(self, snap: CircuitSnapshot) -> CircuitSnapshot:
        if snap.state == CircuitState.OPEN:
            elapsed = time.monotonic() - snap.last_failure_time
            if elapsed >= self._config.recovery_timeout:
                snap.state = CircuitState.HALF_OPEN
                snap.half_open_calls = 0
        return snap

    async def _load_snapshot(self) -> CircuitSnapshot:
        snap = await self._store.load(self._name)
        if snap.state == CircuitState.OPEN:
            elapsed = time.monotonic() - snap.last_failure_time
            if elapsed >= self._config.recovery_timeout:
                snap.state = CircuitState.HALF_OPEN
                snap.half_open_calls = 0
        return snap

    async def call(
        self,
        func: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        lock = self._get_lock()
        async with lock:
            snap = await self._load_snapshot()

            if snap.state == CircuitState.OPEN:
                raise CircuitOpenError(
                    f"Circuit is open; retry after "
                    f"{self._config.recovery_timeout}s"
                )

            if snap.state == CircuitState.HALF_OPEN:
                if snap.half_open_calls >= self._config.half_open_max_calls:
                    raise CircuitOpenError("Half-open call limit reached")
                snap.half_open_calls += 1
                await self._store.save(self._name, snap)

            self._sync_snapshot(snap)

        try:
            result = await func(*args, **kwargs)
        except Exception as exc:
            if self._should_trip(exc):
                async with lock:
                    snap = await self._load_snapshot()
                    self._record_failure(snap)
                    await self._store.save(self._name, snap)
                    self._sync_snapshot(snap)
            raise

        async with lock:
            snap = await self._load_snapshot()
            self._record_success(snap)
            await self._store.save(self._name, snap)
            self._sync_snapshot(snap)
        return result

    def _record_failure(self, snap: CircuitSnapshot) -> None:
        snap.failure_count += 1
        snap.last_failure_time = time.monotonic()
        if snap.state == CircuitState.HALF_OPEN:
            snap.state = CircuitState.OPEN
            snap.failure_count = 0
        elif snap.failure_count >= self._config.failure_threshold:
            snap.state = CircuitState.OPEN

    def _record_success(self, snap: CircuitSnapshot) -> None:
        snap.failure_count = 0
        snap.state = CircuitState.CLOSED
        snap.half_open_calls = 0

    def _sync_snapshot(self, snap: CircuitSnapshot) -> None:
        self._sync_state = snap.state
        self._sync_failure_count = snap.failure_count
        self._sync_last_failure_time = snap.last_failure_time
        self._sync_half_open_calls = snap.half_open_calls

    _sync_state: CircuitState = CircuitState.CLOSED
    _sync_failure_count: int = 0
    _sync_last_failure_time: float = 0.0
    _sync_half_open_calls: int = 0


class TenantCircuitBreakerRegistry:
    def __init__(
        self,
        config: CircuitBreakerConfig,
        name_prefix: str = "default",
        max_tenants: int = 1000,
    ) -> None:
        self._config = config
        self._name_prefix = name_prefix
        self._max_tenants = max_tenants
        self._breakers: OrderedDict[str, CircuitBreaker] = OrderedDict()
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def for_tenant(self, tenant_id: str) -> CircuitBreaker:
        lock = self._get_lock()
        async with lock:
            if tenant_id in self._breakers:
                self._breakers.move_to_end(tenant_id)
                return self._breakers[tenant_id]
            if len(self._breakers) >= self._max_tenants:
                self._breakers.popitem(last=False)
            breaker = CircuitBreaker(
                config=self._config,
                name=f"{self._name_prefix}-{tenant_id}",
            )
            self._breakers[tenant_id] = breaker
            return breaker


class GlobalProviderBreaker:
    def __init__(
        self,
        registry: TenantCircuitBreakerRegistry,
        global_config: CircuitBreakerConfig | None = None,
    ) -> None:
        self._registry = registry
        cfg = global_config or CircuitBreakerConfig(
            failure_threshold=5,
            recovery_timeout=60.0,
            half_open_max_calls=1,
        )
        self._global = CircuitBreaker(
            config=cfg,
            name="global-provider",
            should_trip=is_provider_rate_limit,
        )

    @property
    def global_state(self) -> CircuitState:
        return self._global.state

    async def call(
        self,
        tenant_id: str,
        func: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        async def _guarded() -> T:
            tenant_breaker = await self._registry.for_tenant(tenant_id)
            return await tenant_breaker.call(func, *args, **kwargs)

        return await self._global.call(_guarded)
