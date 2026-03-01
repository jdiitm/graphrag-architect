from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Protocol, Tuple, runtime_checkable

from orchestrator.app.ast_result_consumer import ASTResultConsumer, RemoteASTResult
from orchestrator.app.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
)
from orchestrator.app.config import GRPCASTConfig


@runtime_checkable
class ASTTransport(Protocol):
    async def send_batch(
        self, requests: List[Dict[str, str]],
    ) -> List[Dict[str, Any]]: ...


class GRPCASTClient:
    def __init__(
        self,
        config: GRPCASTConfig,
        transport: Optional[ASTTransport] = None,
    ) -> None:
        self._config = config
        self._transport = transport
        self._breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=config.max_retries,
                recovery_timeout=config.timeout_seconds,
                jitter_factor=0.0,
            ),
            name="ast-grpc",
        )

    @property
    def is_available(self) -> bool:
        return (
            bool(self._config.endpoint)
            and self._breaker.state != CircuitState.OPEN
        )

    async def extract_batch(
        self, files: List[Tuple[str, str]],
    ) -> List[RemoteASTResult]:
        if not files:
            return []

        if self._transport is None:
            raise ConnectionError(
                f"No transport configured for gRPC endpoint "
                f"{self._config.endpoint}"
            )

        requests = [
            {"path": path, "content": content}
            for path, content in files
        ]

        async def _do_send() -> List[Dict[str, Any]]:
            return await self._transport.send_batch(requests)

        raw_results = await self._breaker.call(_do_send)
        return [
            ASTResultConsumer.deserialize(json.dumps(r))
            for r in raw_results
        ]
