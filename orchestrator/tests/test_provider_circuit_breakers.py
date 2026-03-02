import pytest

from orchestrator.app.circuit_breaker import (
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    GlobalProviderBreaker,
    InMemoryStateStore,
    ProviderBreakerRegistry,
    TenantCircuitBreakerRegistry,
)


def _build_breaker(
    tenant_threshold=10,
    provider_threshold=2,
    global_threshold=2,
):
    store = InMemoryStateStore()
    return GlobalProviderBreaker(
        registry=TenantCircuitBreakerRegistry(
            config=CircuitBreakerConfig(
                failure_threshold=tenant_threshold,
                recovery_timeout=60.0,
                jitter_factor=0.0,
            ),
            store=store,
        ),
        global_config=CircuitBreakerConfig(
            failure_threshold=global_threshold,
            recovery_timeout=60.0,
            jitter_factor=0.0,
        ),
        store=store,
        provider_registry=ProviderBreakerRegistry(
            config=CircuitBreakerConfig(
                failure_threshold=provider_threshold,
                recovery_timeout=60.0,
                jitter_factor=0.0,
            ),
            store=store,
        ),
    )


@pytest.mark.asyncio
async def test_provider_a_failure_does_not_affect_provider_b():
    breaker = _build_breaker(provider_threshold=2)

    async def gemini_fail():
        raise ValueError("Gemini API error")

    for _ in range(2):
        with pytest.raises(ValueError):
            await breaker.call("t1", gemini_fail, provider_name="gemini")

    with pytest.raises(CircuitOpenError):
        await breaker.call("t1", gemini_fail, provider_name="gemini")

    async def claude_ok():
        return "claude response"

    result = await breaker.call("t1", claude_ok, provider_name="claude")
    assert result == "claude response"


@pytest.mark.asyncio
async def test_tenant_breaker_still_applies():
    breaker = _build_breaker(tenant_threshold=2, provider_threshold=100)

    async def fail_fn():
        raise ValueError("tenant error")

    for _ in range(2):
        with pytest.raises(ValueError):
            await breaker.call("bad-tenant", fail_fn, provider_name="claude")

    with pytest.raises(CircuitOpenError):
        await breaker.call("bad-tenant", fail_fn, provider_name="claude")

    async def ok_fn():
        return "ok"

    result = await breaker.call("good-tenant", ok_fn, provider_name="claude")
    assert result == "ok"


@pytest.mark.asyncio
async def test_global_breaker_only_for_infrastructure():
    breaker = _build_breaker(
        tenant_threshold=100,
        provider_threshold=100,
        global_threshold=2,
    )

    async def app_error_fn():
        raise ValueError("application error")

    for _ in range(5):
        with pytest.raises(ValueError):
            await breaker.call("t1", app_error_fn, provider_name="claude")

    assert breaker.global_state != CircuitState.OPEN

    async def infra_error_fn():
        raise ConnectionError("infrastructure down")

    for _ in range(2):
        with pytest.raises(ConnectionError):
            await breaker.call("t1", infra_error_fn, provider_name="claude")

    assert breaker.global_state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_provider_recovery_independent():
    breaker = _build_breaker(provider_threshold=2)

    async def fail_fn():
        raise ValueError("error")

    for _ in range(2):
        with pytest.raises(ValueError):
            await breaker.call("t1", fail_fn, provider_name="gemini")

    with pytest.raises(CircuitOpenError):
        await breaker.call("t1", fail_fn, provider_name="gemini")

    async def ok_fn():
        return "ok"

    result = await breaker.call("t1", ok_fn, provider_name="claude")
    assert result == "ok"

    with pytest.raises(CircuitOpenError):
        await breaker.call("t1", ok_fn, provider_name="gemini")
