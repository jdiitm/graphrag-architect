from __future__ import annotations

from dataclasses import dataclass, field

from orchestrator.app.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    InMemoryStateStore,
    RedisStateStore,
    StateStore,
)
from orchestrator.app.config import AuthConfig, ExtractionConfig, RedisConfig
from orchestrator.app.telemetry_ports import (
    MetricsPort,
    NoopMetricsPort,
    NoopTracingPort,
    TracingPort,
)


def _build_state_store() -> StateStore:
    redis_cfg = RedisConfig.from_env()
    if redis_cfg.url:
        return RedisStateStore(
            url=redis_cfg.url,
            key_prefix=redis_cfg.key_prefix,
            password=redis_cfg.password,
            db=redis_cfg.db,
        )
    return InMemoryStateStore()


@dataclass(frozen=True)
class AppContainer:
    circuit_breaker: CircuitBreaker
    auth_config: AuthConfig
    extraction_config: ExtractionConfig
    tracing_port: TracingPort = field(default_factory=NoopTracingPort)
    metrics_port: MetricsPort = field(default_factory=NoopMetricsPort)

    @classmethod
    def from_env(cls) -> AppContainer:
        store = _build_state_store()
        return cls(
            circuit_breaker=CircuitBreaker(CircuitBreakerConfig(), store=store),
            auth_config=AuthConfig.from_env(),
            extraction_config=ExtractionConfig.from_env(),
        )
