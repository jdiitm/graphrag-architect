from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import List

from orchestrator.app.extraction_models import (
    CallsEdge,
    ServiceExtractionResult,
    ServiceNode,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FunctionInfo:
    name: str
    exported: bool = False
    parameters: int = 0


@dataclass(frozen=True)
class HTTPCallInfo:
    method: str
    path_hint: str = ""


@dataclass(frozen=True)
class RemoteASTResult:
    file_path: str
    language: str
    package_name: str = ""
    functions: List[FunctionInfo] = field(default_factory=list)
    imports: List[str] = field(default_factory=list)
    http_calls: List[HTTPCallInfo] = field(default_factory=list)
    service_hints: List[str] = field(default_factory=list)
    http_handlers: List[str] = field(default_factory=list)
    source_type: str = "source_code"


def _derive_service_id(file_path: str) -> str:
    parts = file_path.replace("\\", "/").split("/")
    if len(parts) >= 2:
        return parts[-2]
    return os.path.splitext(parts[-1])[0]


def _detect_framework_from_result(result: RemoteASTResult) -> str:
    if "http-server" in result.service_hints:
        return "net/http"
    if "grpc-server" in result.service_hints:
        return "grpc"
    if result.http_handlers:
        return "unknown"
    return "unknown"


class ASTResultConsumer:
    @staticmethod
    def deserialize(raw: bytes | str) -> RemoteASTResult:
        data = json.loads(raw)

        functions = [
            FunctionInfo(
                name=f["name"],
                exported=f.get("exported", False),
                parameters=f.get("parameters", 0),
            )
            for f in data.get("functions", [])
        ]

        http_calls = [
            HTTPCallInfo(
                method=c["method"],
                path_hint=c.get("path_hint", ""),
            )
            for c in data.get("http_calls", [])
        ]

        return RemoteASTResult(
            file_path=data["file_path"],
            language=data["language"],
            package_name=data.get("package_name", ""),
            functions=functions,
            imports=data.get("imports", []),
            http_calls=http_calls,
            service_hints=data.get("service_hints", []),
            http_handlers=data.get("http_handlers", []),
            source_type=data.get("source_type", "source_code"),
        )

    @staticmethod
    def convert_to_extraction_models(
        result: RemoteASTResult,
        tenant_id: str = "default",
    ) -> ServiceExtractionResult:
        service_id = _derive_service_id(result.file_path)
        is_server = bool(result.service_hints) or bool(result.http_handlers)

        services: List[ServiceNode] = []
        if is_server:
            framework = _detect_framework_from_result(result)
            services.append(ServiceNode(
                id=service_id,
                name=result.package_name or service_id,
                language=result.language,
                framework=framework,
                opentelemetry_enabled=False,
                tenant_id=tenant_id,
            ))

        calls: List[CallsEdge] = []
        for http_call in result.http_calls:
            target = http_call.path_hint or "unknown"
            calls.append(CallsEdge(
                source_service_id=service_id,
                target_service_id=target,
                protocol="http",
                tenant_id=tenant_id,
            ))

        return ServiceExtractionResult(services=services, calls=calls)
