import asyncio
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    CircuitOpenError,
)


class TestCircuitBreakerConfig:
    def test_defaults(self):
        cfg = CircuitBreakerConfig()
        assert cfg.failure_threshold == 3
        assert cfg.recovery_timeout == 30.0
        assert cfg.half_open_max_calls == 1

    def test_custom(self):
        cfg = CircuitBreakerConfig(
            failure_threshold=5, recovery_timeout=10.0, half_open_max_calls=2
        )
        assert cfg.failure_threshold == 5
        assert cfg.recovery_timeout == 10.0
        assert cfg.half_open_max_calls == 2


class TestCircuitBreakerState:
    def test_initial_state_is_closed(self):
        cb = CircuitBreaker(CircuitBreakerConfig())
        assert cb.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_success_keeps_closed(self):
        cb = CircuitBreaker(CircuitBreakerConfig())
        await cb.call(AsyncMock(return_value="ok"))
        assert cb.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_single_failure_stays_closed(self):
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))
        failing = AsyncMock(side_effect=ConnectionError("down"))
        with pytest.raises(ConnectionError):
            await cb.call(failing)
        assert cb.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_threshold_failures_opens_circuit(self):
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))
        failing = AsyncMock(side_effect=ConnectionError("down"))
        for _ in range(3):
            with pytest.raises(ConnectionError):
                await cb.call(failing)
        assert cb.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_open_circuit_rejects_immediately(self):
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1))
        failing = AsyncMock(side_effect=ConnectionError("down"))
        with pytest.raises(ConnectionError):
            await cb.call(failing)
        assert cb.state == CircuitState.OPEN

        with pytest.raises(CircuitOpenError):
            await cb.call(AsyncMock())

    @pytest.mark.asyncio
    async def test_half_open_after_timeout(self):
        cb = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.05)
        )
        failing = AsyncMock(side_effect=ConnectionError("down"))
        with pytest.raises(ConnectionError):
            await cb.call(failing)
        assert cb.state == CircuitState.OPEN

        await asyncio.sleep(0.06)
        assert cb.state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_success_closes_circuit(self):
        cb = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.05)
        )
        failing = AsyncMock(side_effect=ConnectionError("down"))
        with pytest.raises(ConnectionError):
            await cb.call(failing)

        await asyncio.sleep(0.06)
        result = await cb.call(AsyncMock(return_value="recovered"))
        assert result == "recovered"
        assert cb.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_half_open_failure_reopens_circuit(self):
        cb = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.05)
        )
        failing = AsyncMock(side_effect=ConnectionError("down"))
        with pytest.raises(ConnectionError):
            await cb.call(failing)

        await asyncio.sleep(0.06)
        assert cb.state == CircuitState.HALF_OPEN

        with pytest.raises(ConnectionError):
            await cb.call(AsyncMock(side_effect=ConnectionError("still down")))
        assert cb.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_success_resets_failure_count(self):
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))
        failing = AsyncMock(side_effect=ConnectionError("down"))

        for _ in range(2):
            with pytest.raises(ConnectionError):
                await cb.call(failing)

        await cb.call(AsyncMock(return_value="ok"))
        assert cb.state == CircuitState.CLOSED

        for _ in range(2):
            with pytest.raises(ConnectionError):
                await cb.call(failing)
        assert cb.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_call_returns_result(self):
        cb = CircuitBreaker(CircuitBreakerConfig())
        result = await cb.call(AsyncMock(return_value=42))
        assert result == 42

    @pytest.mark.asyncio
    async def test_call_passes_args(self):
        cb = CircuitBreaker(CircuitBreakerConfig())
        func = AsyncMock(return_value="done")
        await cb.call(func, "a", key="b")
        func.assert_awaited_once_with("a", key="b")
