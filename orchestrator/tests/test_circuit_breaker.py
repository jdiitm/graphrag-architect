import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitSnapshot,
    CircuitState,
    CircuitOpenError,
    InMemoryStateStore,
    RedisStateStore,
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


class TestRedisStateStore:
    @pytest.mark.asyncio
    async def test_load_returns_default_when_empty(self):
        mock_redis = AsyncMock()
        mock_redis.hgetall = AsyncMock(return_value={})
        with patch("redis.asyncio.from_url", return_value=mock_redis):
            store = RedisStateStore(url="redis://localhost:6379")
        snap = await store.load("test-circuit")
        assert snap.state == CircuitState.CLOSED
        assert snap.failure_count == 0

    @pytest.mark.asyncio
    async def test_save_writes_hash_with_ttl(self):
        mock_pipe = AsyncMock()
        mock_pipe.hset = AsyncMock()
        mock_pipe.expire = AsyncMock()
        mock_pipe.execute = AsyncMock(return_value=[True, True])

        mock_redis = AsyncMock()
        mock_redis.pipeline = lambda: mock_pipe
        with patch("redis.asyncio.from_url", return_value=mock_redis):
            store = RedisStateStore(url="redis://localhost:6379", ttl_seconds=120)
        snap = CircuitSnapshot(
            state=CircuitState.OPEN,
            failure_count=5,
            last_failure_time=1000.0,
            half_open_calls=0,
        )
        await store.save("test-circuit", snap)
        mock_pipe.hset.assert_called_once()
        mock_pipe.expire.assert_called_once()

    @pytest.mark.asyncio
    async def test_load_parses_stored_data(self):
        mock_redis = AsyncMock()
        mock_redis.hgetall = AsyncMock(return_value={
            "state": "open",
            "failure_count": "3",
            "last_failure_time": "99.5",
            "half_open_calls": "0",
        })
        with patch("redis.asyncio.from_url", return_value=mock_redis):
            store = RedisStateStore(url="redis://localhost:6379")
        snap = await store.load("test-circuit")
        assert snap.state == CircuitState.OPEN
        assert snap.failure_count == 3
        assert snap.last_failure_time == 99.5


class TestContainerStoreSelection:
    def test_selects_inmemory_when_no_redis_url(self):
        from orchestrator.app.container import _build_state_store
        import os
        os.environ.pop("REDIS_URL", None)
        store = _build_state_store()
        assert isinstance(store, InMemoryStateStore)

    def test_selects_redis_when_url_set(self):
        from orchestrator.app.container import _build_state_store
        with patch.dict("os.environ", {"REDIS_URL": "redis://localhost:6379"}):
            with patch("redis.asyncio.from_url") as mock_from_url:
                mock_from_url.return_value = AsyncMock()
                store = _build_state_store()
        assert isinstance(store, RedisStateStore)
