from __future__ import annotations

import asyncio
import enum
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol, TypeVar, runtime_checkable

T = TypeVar("T")


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


class CircuitBreaker:
    def __init__(
        self,
        config: CircuitBreakerConfig,
        store: StateStore | None = None,
        name: str = "default",
    ) -> None:
        self._config = config
        self._store: StateStore = store or InMemoryStateStore()
        self._name = name
        self._lock: asyncio.Lock | None = None

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
        except Exception:
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
