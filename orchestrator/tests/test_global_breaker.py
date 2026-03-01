from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    GlobalProviderBreaker,
    TenantCircuitBreakerRegistry,
    is_provider_rate_limit,
)


class TestIsProviderRateLimit:
    def test_detects_http_429(self) -> None:
        exc = Exception("HTTP 429 Too Many Requests")
        assert is_provider_rate_limit(exc) is True

    def test_detects_rate_limit_phrase(self) -> None:
        exc = Exception("rate limit exceeded for model")
        assert is_provider_rate_limit(exc) is True

    def test_detects_resource_exhausted(self) -> None:
        exc = Exception("google.api_core: 429 RESOURCE_EXHAUSTED")
        assert is_provider_rate_limit(exc) is True

    def test_detects_quota_exceeded(self) -> None:
        exc = Exception("Quota exceeded for project")
        assert is_provider_rate_limit(exc) is True

    def test_rejects_connection_error(self) -> None:
        exc = ConnectionError("Connection refused")
        assert is_provider_rate_limit(exc) is False

    def test_rejects_generic_error(self) -> None:
        exc = ValueError("invalid input")
        assert is_provider_rate_limit(exc) is False


class TestCircuitBreakerShouldTrip:
    @pytest.mark.asyncio
    async def test_default_trips_on_any_exception(self) -> None:
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1))
        with pytest.raises(ValueError):
            await cb.call(AsyncMock(side_effect=ValueError("bad")))
        assert cb.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_custom_filter_ignores_non_matching_errors(self) -> None:
        cb = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=1),
            should_trip=lambda exc: "fatal" in str(exc),
        )
        with pytest.raises(ValueError):
            await cb.call(AsyncMock(side_effect=ValueError("minor issue")))
        assert cb.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_custom_filter_trips_on_matching_errors(self) -> None:
        cb = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=1),
            should_trip=lambda exc: "fatal" in str(exc),
        )
        with pytest.raises(ValueError):
            await cb.call(AsyncMock(side_effect=ValueError("fatal crash")))
        assert cb.state == CircuitState.OPEN


@pytest.mark.asyncio
class TestGlobalProviderBreaker:
    async def test_passes_through_when_closed(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=3),
        )
        global_breaker = GlobalProviderBreaker(registry=registry)

        result = await global_breaker.call(
            "tenant-1", AsyncMock(return_value="ok"),
        )
        assert result == "ok"

    async def test_trips_globally_on_network_failure(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=10),
        )
        global_breaker = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(
                failure_threshold=2, recovery_timeout=60.0,
            ),
        )

        for _ in range(2):
            with pytest.raises(ConnectionError):
                await global_breaker.call(
                    "tenant-1",
                    AsyncMock(side_effect=ConnectionError("network down")),
                )

        assert global_breaker.global_state == CircuitState.OPEN

    async def test_global_open_rejects_all_tenants(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=10),
        )
        global_breaker = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(
                failure_threshold=1, recovery_timeout=60.0,
            ),
        )

        with pytest.raises(ConnectionError):
            await global_breaker.call(
                "tenant-1",
                AsyncMock(side_effect=ConnectionError("network down")),
            )

        with pytest.raises(CircuitOpenError):
            await global_breaker.call(
                "tenant-2", AsyncMock(return_value="ok"),
            )

        with pytest.raises(CircuitOpenError):
            await global_breaker.call(
                "tenant-3", AsyncMock(return_value="ok"),
            )

    async def test_non_network_errors_do_not_trip_global(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=10),
        )
        global_breaker = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(
                failure_threshold=1, recovery_timeout=60.0,
            ),
        )

        for _ in range(5):
            with pytest.raises(ValueError):
                await global_breaker.call(
                    "tenant-1",
                    AsyncMock(side_effect=ValueError("invalid input")),
                )

        assert global_breaker.global_state == CircuitState.CLOSED

    async def test_half_open_recovery_on_success(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=10),
        )
        global_breaker = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(
                failure_threshold=1, recovery_timeout=0.05,
            ),
        )

        with pytest.raises(ConnectionError):
            await global_breaker.call(
                "tenant-1",
                AsyncMock(side_effect=ConnectionError("network down")),
            )
        assert global_breaker.global_state == CircuitState.OPEN

        await asyncio.sleep(0.06)
        assert global_breaker.global_state == CircuitState.HALF_OPEN

        result = await global_breaker.call(
            "tenant-1", AsyncMock(return_value="recovered"),
        )
        assert result == "recovered"
        assert global_breaker.global_state == CircuitState.CLOSED

    async def test_half_open_failure_reopens(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=10),
        )
        global_breaker = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(
                failure_threshold=1, recovery_timeout=0.05,
            ),
        )

        with pytest.raises(ConnectionError):
            await global_breaker.call(
                "tenant-1",
                AsyncMock(side_effect=ConnectionError("network down")),
            )

        await asyncio.sleep(0.06)
        assert global_breaker.global_state == CircuitState.HALF_OPEN

        with pytest.raises(ConnectionError):
            await global_breaker.call(
                "tenant-1",
                AsyncMock(side_effect=ConnectionError("still down")),
            )
        assert global_breaker.global_state == CircuitState.OPEN

    async def test_tenant_circuit_open_does_not_trip_global(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=1),
        )
        global_breaker = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(
                failure_threshold=1, recovery_timeout=60.0,
            ),
        )

        with pytest.raises(ValueError):
            await global_breaker.call(
                "tenant-1",
                AsyncMock(side_effect=ValueError("invalid input")),
            )

        with pytest.raises(CircuitOpenError):
            await global_breaker.call(
                "tenant-1", AsyncMock(return_value="ok"),
            )

        assert global_breaker.global_state == CircuitState.CLOSED
