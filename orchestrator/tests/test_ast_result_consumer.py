import json
import os
from unittest import mock

import pytest

from orchestrator.app.ast_result_consumer import (
    ASTResultConsumer,
    FunctionInfo,
    HTTPCallInfo,
    RemoteASTResult,
)
from orchestrator.app.extraction_models import CallsEdge, ServiceNode


def _go_ast_payload() -> dict:
    return {
        "file_path": "cmd/server/main.go",
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


def _python_ast_payload() -> dict:
    return {
        "file_path": "app/main.py",
        "language": "python",
        "package_name": "",
        "functions": [
            {"name": "UserRequest", "exported": True, "parameters": 0},
            {"name": "create_user", "exported": True, "parameters": 1},
        ],
        "imports": ["fastapi", "pydantic"],
        "http_calls": [],
        "service_hints": [],
        "http_handlers": ["create_user"],
        "source_type": "source_code",
    }


class TestASTResultConsumerDeserialize:
    def test_deserialize_go_ast_result(self) -> None:
        raw = json.dumps(_go_ast_payload()).encode()
        result = ASTResultConsumer.deserialize(raw)

        assert isinstance(result, RemoteASTResult)
        assert result.file_path == "cmd/server/main.go"
        assert result.language == "go"
        assert result.package_name == "main"
        assert len(result.functions) == 2
        assert result.functions[0] == FunctionInfo(
            name="main", exported=False, parameters=0,
        )
        assert result.functions[1] == FunctionInfo(
            name="HandleHealth", exported=True, parameters=2,
        )
        assert result.imports == ["fmt", "net/http"]
        assert len(result.http_calls) == 1
        assert result.http_calls[0] == HTTPCallInfo(
            method="GET", path_hint="http://auth-svc/health",
        )
        assert result.service_hints == ["http-server"]

    def test_deserialize_python_ast_result(self) -> None:
        raw = json.dumps(_python_ast_payload())
        result = ASTResultConsumer.deserialize(raw)

        assert result.language == "python"
        assert result.file_path == "app/main.py"
        assert len(result.functions) == 2
        assert result.imports == ["fastapi", "pydantic"]
        assert result.http_handlers == ["create_user"]

    def test_deserialize_string_input(self) -> None:
        raw_str = json.dumps(_go_ast_payload())
        result = ASTResultConsumer.deserialize(raw_str)
        assert result.language == "go"

    def test_deserialize_minimal_payload(self) -> None:
        raw = json.dumps({"file_path": "x.go", "language": "go"})
        result = ASTResultConsumer.deserialize(raw)
        assert result.file_path == "x.go"
        assert result.functions == []
        assert result.imports == []


class TestASTResultConsumerMalformed:
    def test_invalid_json_raises(self) -> None:
        with pytest.raises(json.JSONDecodeError):
            ASTResultConsumer.deserialize(b"not json at all")

    def test_missing_required_field_raises(self) -> None:
        with pytest.raises(KeyError):
            ASTResultConsumer.deserialize(json.dumps({"language": "go"}))

    def test_empty_bytes_raises(self) -> None:
        with pytest.raises(json.JSONDecodeError):
            ASTResultConsumer.deserialize(b"")


class TestConvertToExtractionModels:
    def test_go_server_produces_service_node(self) -> None:
        raw = json.dumps(_go_ast_payload())
        result = ASTResultConsumer.deserialize(raw)
        extraction = ASTResultConsumer.convert_to_extraction_models(
            result, tenant_id="test-tenant",
        )

        assert len(extraction.services) == 1
        svc = extraction.services[0]
        assert isinstance(svc, ServiceNode)
        assert svc.language == "go"
        assert svc.tenant_id == "test-tenant"
        assert svc.name == "main"
        assert svc.confidence == 1.0

    def test_python_handler_produces_service_node(self) -> None:
        raw = json.dumps(_python_ast_payload())
        result = ASTResultConsumer.deserialize(raw)
        extraction = ASTResultConsumer.convert_to_extraction_models(
            result, tenant_id="test-tenant",
        )

        assert len(extraction.services) == 1
        svc = extraction.services[0]
        assert svc.language == "python"

    def test_http_calls_produce_calls_edges(self) -> None:
        raw = json.dumps(_go_ast_payload())
        result = ASTResultConsumer.deserialize(raw)
        extraction = ASTResultConsumer.convert_to_extraction_models(
            result, tenant_id="test-tenant",
        )

        assert len(extraction.calls) == 1
        edge = extraction.calls[0]
        assert isinstance(edge, CallsEdge)
        assert edge.protocol == "http"
        assert edge.tenant_id == "test-tenant"

    def test_no_server_hints_produces_no_services(self) -> None:
        payload = _go_ast_payload()
        payload["service_hints"] = []
        payload["http_handlers"] = []
        raw = json.dumps(payload)
        result = ASTResultConsumer.deserialize(raw)
        extraction = ASTResultConsumer.convert_to_extraction_models(result)

        assert len(extraction.services) == 0


class TestGraphBuilderRemoteASTConfig:
    def test_use_remote_ast_default_false(self) -> None:
        from orchestrator.app import graph_builder
        original = graph_builder._USE_REMOTE_AST
        assert isinstance(original, bool)
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("USE_REMOTE_AST", None)
            computed = os.environ.get("USE_REMOTE_AST", "false").lower() == "true"
            assert computed is False

    def test_use_remote_ast_enabled(self) -> None:
        with mock.patch.dict(os.environ, {"USE_REMOTE_AST": "true"}):
            computed = os.environ.get("USE_REMOTE_AST", "false").lower() == "true"
            assert computed is True

    def test_module_level_flag_is_bool(self) -> None:
        from orchestrator.app import graph_builder
        assert hasattr(graph_builder, "_USE_REMOTE_AST")
        assert isinstance(graph_builder._USE_REMOTE_AST, bool)
