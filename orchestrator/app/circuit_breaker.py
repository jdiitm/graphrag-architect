from __future__ import annotations

import enum
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, TypeVar

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


class CircuitBreaker:
    def __init__(self, config: CircuitBreakerConfig) -> None:
        self._config = config
        self._failure_count: int = 0
        self._last_failure_time: float = 0.0
        self._state: CircuitState = CircuitState.CLOSED
        self._half_open_calls: int = 0

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self._config.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
        return self._state

    async def call(
        self,
        func: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        current_state = self.state

        if current_state == CircuitState.OPEN:
            raise CircuitOpenError(
                f"Circuit is open; retry after {self._config.recovery_timeout}s"
            )

        if current_state == CircuitState.HALF_OPEN:
            if self._half_open_calls >= self._config.half_open_max_calls:
                raise CircuitOpenError("Half-open call limit reached")
            self._half_open_calls += 1

        try:
            result = await func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise

        self._record_success()
        return result

    def _record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.monotonic()
        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            self._failure_count = 0
        elif self._failure_count >= self._config.failure_threshold:
            self._state = CircuitState.OPEN

    def _record_success(self) -> None:
        self._failure_count = 0
        self._state = CircuitState.CLOSED
        self._half_open_calls = 0
