from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.circuit_breaker import (
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    GlobalProviderBreaker,
    TenantCircuitBreakerRegistry,
)


def _rate_limit_error() -> RuntimeError:
    return RuntimeError("429 rate limit exceeded")



class TestProviderErrorsDoNotTripGlobal:

    @pytest.mark.asyncio
    async def test_429_does_not_trip_global_breaker(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=10),
            name_prefix="test",
        )
        gpb = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(failure_threshold=2),
        )
        failing = AsyncMock(side_effect=_rate_limit_error())
        for _ in range(3):
            with pytest.raises(RuntimeError, match="429"):
                await gpb.call("tenant-a", failing)
        assert gpb.global_state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_tenant_a_exhausted_does_not_affect_tenant_b(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=2),
            name_prefix="iso",
        )
        gpb = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(failure_threshold=2),
        )
        failing = AsyncMock(side_effect=_rate_limit_error())
        for _ in range(2):
            with pytest.raises(RuntimeError, match="429"):
                await gpb.call("tenant-a", failing)

        result = await gpb.call("tenant-b", AsyncMock(return_value="healthy"))
        assert result == "healthy"

    @pytest.mark.asyncio
    async def test_tenant_b_succeeds_when_tenant_a_breaker_open(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=2),
            name_prefix="open",
        )
        gpb = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(failure_threshold=2),
        )
        failing = AsyncMock(side_effect=_rate_limit_error())
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await gpb.call("tenant-a", failing)

        success_func = AsyncMock(return_value="ok")
        result = await gpb.call("tenant-b", success_func)
        assert result == "ok"
        success_func.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_provider_errors_only_affect_specific_tenant(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=2),
            name_prefix="scope",
        )
        gpb = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(failure_threshold=2),
        )
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await gpb.call(
                    "tenant-a", AsyncMock(side_effect=_rate_limit_error()),
                )

        with pytest.raises(CircuitOpenError):
            await gpb.call("tenant-a", AsyncMock(return_value="no"))

        assert gpb.global_state == CircuitState.CLOSED
        result = await gpb.call("tenant-b", AsyncMock(return_value="fine"))
        assert result == "fine"


class TestNetworkErrorsTripGlobal:

    @pytest.mark.asyncio
    async def test_connection_error_increments_global(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=5),
            name_prefix="net",
        )
        gpb = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(failure_threshold=2),
        )
        failing = AsyncMock(side_effect=ConnectionError("refused"))
        for _ in range(2):
            with pytest.raises(ConnectionError):
                await gpb.call("tenant-a", failing)
        assert gpb.global_state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_os_error_increments_global(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=5),
            name_prefix="net",
        )
        gpb = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(failure_threshold=2),
        )
        failing = AsyncMock(side_effect=OSError("network unreachable"))
        for _ in range(2):
            with pytest.raises(OSError):
                await gpb.call("tenant-a", failing)
        assert gpb.global_state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_timeout_error_increments_global(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=5),
            name_prefix="net",
        )
        gpb = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(failure_threshold=2),
        )
        failing = AsyncMock(side_effect=TimeoutError("timed out"))
        for _ in range(2):
            with pytest.raises(TimeoutError):
                await gpb.call("tenant-a", failing)
        assert gpb.global_state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_global_only_opens_from_network_not_429(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=10),
            name_prefix="mix",
        )
        gpb = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(failure_threshold=3),
        )
        for _ in range(5):
            with pytest.raises(RuntimeError, match="429"):
                await gpb.call(
                    "tenant-x", AsyncMock(side_effect=_rate_limit_error()),
                )
        assert gpb.global_state == CircuitState.CLOSED

        for _ in range(3):
            with pytest.raises(ConnectionError):
                await gpb.call(
                    "tenant-y",
                    AsyncMock(side_effect=ConnectionError("down")),
                )
        assert gpb.global_state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_global_open_blocks_all_tenants(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=10),
            name_prefix="block",
        )
        gpb = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(failure_threshold=2),
        )
        for _ in range(2):
            with pytest.raises(ConnectionError):
                await gpb.call(
                    "tenant-a",
                    AsyncMock(side_effect=ConnectionError("down")),
                )
        assert gpb.global_state == CircuitState.OPEN

        with pytest.raises(CircuitOpenError):
            await gpb.call("tenant-a", AsyncMock(return_value="no"))
        with pytest.raises(CircuitOpenError):
            await gpb.call("tenant-b", AsyncMock(return_value="no"))
        with pytest.raises(CircuitOpenError):
            await gpb.call("tenant-c", AsyncMock(return_value="no"))


class TestGlobalHalfOpenProbeLimit:

    @pytest.mark.asyncio
    async def test_half_open_limits_concurrent_probes(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(failure_threshold=5),
            name_prefix="ho",
        )
        gpb = GlobalProviderBreaker(
            registry=registry,
            global_config=CircuitBreakerConfig(
                failure_threshold=2,
                recovery_timeout=0.0,
                jitter_factor=0.0,
                half_open_max_calls=1,
            ),
        )

        for _ in range(2):
            with pytest.raises(ConnectionError):
                await gpb.call(
                    "tenant-a",
                    AsyncMock(side_effect=ConnectionError("down")),
                )
        assert gpb.global_state == CircuitState.HALF_OPEN

        async def slow_success() -> str:
            await asyncio.sleep(0.05)
            return "probe-ok"

        results = await asyncio.gather(
            gpb.call("t1", slow_success),
            gpb.call("t2", slow_success),
            gpb.call("t3", slow_success),
            return_exceptions=True,
        )

        successes = [r for r in results if r == "probe-ok"]
        rejections = [r for r in results if isinstance(r, CircuitOpenError)]
        assert len(successes) == 1
        assert len(rejections) == 2


class TestIsGlobalFailureClassification:

    @pytest.mark.asyncio
    async def test_classifies_exception_types_correctly(self) -> None:
        registry = TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(),
            name_prefix="classify",
        )
        gpb = GlobalProviderBreaker(registry=registry)

        assert gpb._is_global_failure(ConnectionError("refused")) is True
        assert gpb._is_global_failure(OSError("unreachable")) is True
        assert gpb._is_global_failure(TimeoutError("timed out")) is True
        assert gpb._is_global_failure(RuntimeError("429 rate limit")) is False
        assert gpb._is_global_failure(ValueError("bad input")) is False
        assert gpb._is_global_failure(RuntimeError("503 unavailable")) is False
