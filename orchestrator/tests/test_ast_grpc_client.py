from __future__ import annotations

import os
import time
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.ast_grpc_client import ASTTransport, GRPCASTClient
from orchestrator.app.ast_result_consumer import (
    ASTResultConsumer,
    RemoteASTResult,
)
from orchestrator.app.circuit_breaker import CircuitOpenError, CircuitState
from orchestrator.app.config import GRPCASTConfig
from orchestrator.app.extraction_models import ServiceNode


_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


def _make_go_ast_response(
    file_path: str = "services/auth/main.go",
) -> Dict[str, Any]:
    return {
        "file_path": file_path,
        "language": "go",
        "package_name": "main",
        "functions": [
            {"name": "main", "exported": False, "parameters": 0},
            {"name": "HandleHealth", "exported": True, "parameters": 2},
        ],
        "imports": ["fmt", "net/http"],
        "http_calls": [{"method": "GET", "path_hint": "http://auth-svc/health"}],
        "service_hints": ["http-server"],
        "http_handlers": [],
        "source_type": "source_code",
    }


class _MockTransport:
    def __init__(
        self,
        responses: List[Dict[str, Any]] | None = None,
        side_effect: Exception | None = None,
    ) -> None:
        self._responses = responses or []
        self._side_effect = side_effect

    async def send_batch(
        self, requests: List[Dict[str, str]],
    ) -> List[Dict[str, Any]]:
        if self._side_effect is not None:
            raise self._side_effect
        return self._responses


class TestGRPCASTConfig:

    def test_fields_have_correct_defaults(self) -> None:
        cfg = GRPCASTConfig()
        assert cfg.endpoint == ""
        assert cfg.timeout_seconds == 30.0
        assert cfg.max_retries == 3

    def test_from_env_reads_all_variables(self) -> None:
        with patch.dict(os.environ, {
            "AST_GRPC_ENDPOINT": "localhost:50051",
            "AST_GRPC_TIMEOUT": "10",
            "AST_GRPC_MAX_RETRIES": "5",
        }):
            cfg = GRPCASTConfig.from_env()
            assert cfg.endpoint == "localhost:50051"
            assert cfg.timeout_seconds == 10.0
            assert cfg.max_retries == 5

    def test_from_env_uses_defaults_when_unset(self) -> None:
        clean_env = {
            k: v for k, v in os.environ.items()
            if k not in {
                "AST_GRPC_ENDPOINT", "AST_GRPC_TIMEOUT", "AST_GRPC_MAX_RETRIES",
            }
        }
        with patch.dict(os.environ, clean_env, clear=True):
            cfg = GRPCASTConfig.from_env()
            assert cfg.endpoint == ""
            assert cfg.timeout_seconds == 30.0
            assert cfg.max_retries == 3

    def test_config_is_frozen(self) -> None:
        cfg = GRPCASTConfig(endpoint="localhost:50051")
        with pytest.raises(AttributeError):
            cfg.endpoint = "other"  # type: ignore[misc]


class TestGRPCASTClientExtractBatch:

    @pytest.mark.asyncio
    async def test_returns_parsed_remote_ast_results(self) -> None:
        response = _make_go_ast_response()
        transport = _MockTransport(responses=[response])
        cfg = GRPCASTConfig(endpoint="localhost:50051")
        client = GRPCASTClient(config=cfg, transport=transport)

        results = await client.extract_batch([
            ("services/auth/main.go", "package main"),
        ])

        assert len(results) == 1
        assert isinstance(results[0], RemoteASTResult)
        assert results[0].file_path == "services/auth/main.go"
        assert results[0].language == "go"
        assert results[0].package_name == "main"

    @pytest.mark.asyncio
    async def test_connection_failure_propagates(self) -> None:
        transport = _MockTransport(side_effect=ConnectionError("unreachable"))
        cfg = GRPCASTConfig(endpoint="localhost:50051", max_retries=5)
        client = GRPCASTClient(config=cfg, transport=transport)

        with pytest.raises(ConnectionError, match="unreachable"):
            await client.extract_batch([
                ("services/auth/main.go", "package main"),
            ])

    @pytest.mark.asyncio
    async def test_circuit_open_after_repeated_failures(self) -> None:
        transport = _MockTransport(side_effect=ConnectionError("fail"))
        cfg = GRPCASTConfig(
            endpoint="localhost:50051", max_retries=2, timeout_seconds=60.0,
        )
        client = GRPCASTClient(config=cfg, transport=transport)

        for _ in range(2):
            with pytest.raises(ConnectionError):
                await client.extract_batch([("test.go", "package main")])

        with pytest.raises(CircuitOpenError):
            await client.extract_batch([("test.go", "package main")])

    @pytest.mark.asyncio
    async def test_empty_file_list_returns_empty(self) -> None:
        cfg = GRPCASTConfig(endpoint="localhost:50051")
        client = GRPCASTClient(config=cfg)

        results = await client.extract_batch([])
        assert results == []

    @pytest.mark.asyncio
    async def test_no_transport_raises_connection_error(self) -> None:
        cfg = GRPCASTConfig(endpoint="localhost:50051")
        client = GRPCASTClient(config=cfg)

        with pytest.raises(ConnectionError, match="No transport configured"):
            await client.extract_batch([("test.go", "package main")])

    @pytest.mark.asyncio
    async def test_multiple_files_returns_multiple_results(self) -> None:
        responses = [
            _make_go_ast_response("svc-a/main.go"),
            _make_go_ast_response("svc-b/main.go"),
        ]
        transport = _MockTransport(responses=responses)
        cfg = GRPCASTConfig(endpoint="localhost:50051")
        client = GRPCASTClient(config=cfg, transport=transport)

        results = await client.extract_batch([
            ("svc-a/main.go", "package main"),
            ("svc-b/main.go", "package main"),
        ])

        assert len(results) == 2
        assert results[0].file_path == "svc-a/main.go"
        assert results[1].file_path == "svc-b/main.go"


class TestGRPCASTClientIsAvailable:

    def test_available_with_endpoint_and_closed_breaker(self) -> None:
        cfg = GRPCASTConfig(endpoint="localhost:50051")
        client = GRPCASTClient(config=cfg)
        assert client.is_available is True

    def test_not_available_without_endpoint(self) -> None:
        cfg = GRPCASTConfig(endpoint="")
        client = GRPCASTClient(config=cfg)
        assert client.is_available is False

    def test_not_available_with_open_breaker(self) -> None:
        cfg = GRPCASTConfig(endpoint="localhost:50051", timeout_seconds=60.0)
        client = GRPCASTClient(config=cfg)
        client._breaker._sync_state = CircuitState.OPEN
        client._breaker._sync_last_failure_time = time.monotonic()
        assert client.is_available is False


class TestGRPCResultConversion:

    @pytest.mark.asyncio
    async def test_results_convert_to_valid_extraction_models(self) -> None:
        response = _make_go_ast_response()
        transport = _MockTransport(responses=[response])
        cfg = GRPCASTConfig(endpoint="localhost:50051")
        client = GRPCASTClient(config=cfg, transport=transport)

        results = await client.extract_batch([
            ("services/auth/main.go", "package main"),
        ])

        extraction = ASTResultConsumer.convert_to_extraction_models(
            results[0], tenant_id="test-tenant",
        )

        assert len(extraction.services) == 1
        assert isinstance(extraction.services[0], ServiceNode)
        assert extraction.services[0].language == "go"
        assert extraction.services[0].tenant_id == "test-tenant"
        assert len(extraction.calls) == 1
        assert extraction.calls[0].protocol == "http"


class TestGraphBuilderGRPCIntegration:

    @pytest.mark.asyncio
    async def test_grpc_path_when_remote_and_endpoint_configured(self) -> None:
        from orchestrator.app.graph_builder import parse_source_ast

        state = {
            "raw_files": [
                {"path": "services/auth/main.go", "content": "package main"},
            ],
            "extracted_nodes": [],
            "extraction_checkpoint": {},
            "tenant_id": "test-tenant",
        }

        mock_client = AsyncMock()
        mock_client.extract_batch = AsyncMock(return_value=[
            RemoteASTResult(
                file_path="services/auth/main.go",
                language="go",
                package_name="main",
                service_hints=["http-server"],
            ),
        ])

        with (
            patch.dict("os.environ", {
                **_ENV_VARS,
                "AST_GRPC_ENDPOINT": "localhost:50051",
            }),
            patch(
                "orchestrator.app.graph_builder._USE_REMOTE_AST", True,
            ),
            patch(
                "orchestrator.app.graph_builder._get_grpc_ast_client",
                return_value=mock_client,
            ),
        ):
            result = await parse_source_ast(state)

        assert "extracted_nodes" in result
        mock_client.extract_batch.assert_called_once()
        nodes = result["extracted_nodes"]
        service_nodes = [n for n in nodes if isinstance(n, ServiceNode)]
        assert len(service_nodes) == 1
        assert service_nodes[0].language == "go"

    @pytest.mark.asyncio
    async def test_fallback_to_breaker_when_no_endpoint(self) -> None:
        from orchestrator.app.graph_builder import (
            IngestionDegradedError,
            parse_source_ast,
        )

        state = {
            "raw_files": [
                {"path": "services/auth/main.go", "content": "package main"},
            ],
            "extracted_nodes": [],
            "extraction_checkpoint": {},
            "tenant_id": "test-tenant",
        }

        with (
            patch.dict("os.environ", {**_ENV_VARS, "AST_GRPC_ENDPOINT": ""}),
            patch(
                "orchestrator.app.graph_builder._USE_REMOTE_AST", True,
            ),
            patch(
                "orchestrator.app.graph_builder._ast_worker_breaker",
            ) as mock_breaker,
        ):
            mock_breaker.call = AsyncMock(
                side_effect=ConnectionError("Go worker unreachable"),
            )
            mock_breaker._config = MagicMock(recovery_timeout=30.0)
            with pytest.raises(IngestionDegradedError):
                await parse_source_ast(state)

    @pytest.mark.asyncio
    async def test_local_pool_when_remote_ast_false(self) -> None:
        from orchestrator.app.graph_builder import parse_source_ast

        state = {
            "raw_files": [
                {"path": "services/auth/main.go", "content": "package main"},
            ],
            "extracted_nodes": [],
            "extraction_checkpoint": {},
            "tenant_id": "test-tenant",
        }

        pool_was_used = []

        def _tracking_pool():
            pool_was_used.append(True)
            return MagicMock()

        async def _fake_run_in_executor(pool, fn, *args):
            return fn(*args)

        mock_loop = MagicMock()
        mock_loop.run_in_executor = _fake_run_in_executor

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder._USE_REMOTE_AST", False,
            ),
            patch(
                "orchestrator.app.graph_builder._get_process_pool",
                _tracking_pool,
            ),
            patch(
                "asyncio.get_running_loop", return_value=mock_loop,
            ),
        ):
            result = await parse_source_ast(state)

        assert pool_was_used
        assert "extracted_nodes" in result
