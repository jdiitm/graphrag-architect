from __future__ import annotations

from dataclasses import dataclass

from orchestrator.app.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from orchestrator.app.config import AuthConfig, ExtractionConfig


@dataclass(frozen=True)
class AppContainer:
    circuit_breaker: CircuitBreaker
    auth_config: AuthConfig
    extraction_config: ExtractionConfig

    @classmethod
    def from_env(cls) -> AppContainer:
        return cls(
            circuit_breaker=CircuitBreaker(CircuitBreakerConfig()),
            auth_config=AuthConfig.from_env(),
            extraction_config=ExtractionConfig.from_env(),
        )
