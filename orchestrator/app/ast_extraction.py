from __future__ import annotations

import ast
import os
from dataclasses import dataclass, field
from typing import Dict, List

import tree_sitter_go
from tree_sitter import Language, Parser, Node

from orchestrator.app.entity_resolver import resolve_entity_id
from orchestrator.app.extraction_models import (
    CallsEdge,
    ServiceExtractionResult,
    ServiceNode,
)


GO_LANGUAGE = Language(tree_sitter_go.language())

_HTTP_SERVER_PATTERNS = frozenset({
    "ListenAndServe",
    "ListenAndServeTLS",
})

_HTTP_CLIENT_PATTERNS = frozenset({
    "NewRequest",
    "NewRequestWithContext",
    "Get",
    "Post",
    "Head",
    "Do",
})

_GRPC_SERVER_PATTERNS = frozenset({
    "NewServer",
})

_KAFKA_CONSUMER_FUNCS = frozenset({
    "ConsumeTopics",
})

_KAFKA_PRODUCER_FUNCS = frozenset({
    "ProduceTo",
})

_FRAMEWORK_PATTERNS: Dict[str, str] = {
    "gin.Default": "gin",
    "gin.New": "gin",
    "echo.New": "echo",
    "fiber.New": "fiber",
    "chi.NewRouter": "chi",
    "mux.NewRouter": "gorilla",
}


@dataclass
class ASTServiceNode:
    service_id: str
    name: str
    language: str
    framework: str = ""
    opentelemetry_enabled: bool = False
    confidence: float = 1.0


@dataclass
class ASTCallEdge:
    source_service_id: str
    target_hint: str
    protocol: str
    confidence: float = 1.0


@dataclass
class ASTExtractionResult:
    services: List[ASTServiceNode] = field(default_factory=list)
    calls: List[ASTCallEdge] = field(default_factory=list)
    topics_consumed: List[str] = field(default_factory=list)
    topics_produced: List[str] = field(default_factory=list)

    def to_extraction_result(self, tenant_id: str = "default") -> ServiceExtractionResult:
        services = [
            ServiceNode(
                id=s.service_id,
                name=s.name,
                language=s.language,
                framework=s.framework or "unknown",
                opentelemetry_enabled=s.opentelemetry_enabled,
                tenant_id=tenant_id,
            )
            for s in self.services
        ]
        calls = [
            CallsEdge(
                source_service_id=c.source_service_id,
                target_service_id=c.target_hint,
                protocol=c.protocol,
                tenant_id=tenant_id,
            )
            for c in self.calls
        ]
        return ServiceExtractionResult(services=services, calls=calls)


def _derive_service_id(file_path: str) -> str:
    parts = file_path.replace("\\", "/").split("/")
    if len(parts) >= 2:
        name = parts[-2]
        namespace = "/".join(parts[:-2]) if len(parts) > 2 else ""
        return resolve_entity_id(name, namespace=namespace)
    name = os.path.splitext(parts[-1])[0]
    return resolve_entity_id(name)


def _get_package_name(tree: Node) -> str:
    for child in tree.children:
        if child.type == "package_clause":
            for sub in child.children:
                if sub.type == "package_identifier":
                    return sub.text.decode("utf-8")
    return ""


def _find_imports(tree: Node) -> Dict[str, str]:
    imports: Dict[str, str] = {}
    for child in tree.children:
        if child.type == "import_declaration":
            for spec in _walk(child, "import_spec"):
                path_node = spec.child_by_field_name("path")
                if path_node is None:
                    continue
                path_str = path_node.text.decode("utf-8").strip('"')
                alias_node = spec.child_by_field_name("name")
                if alias_node:
                    alias = alias_node.text.decode("utf-8")
                else:
                    alias = path_str.split("/")[-1]
                imports[alias] = path_str
    return imports


def _walk(node: Node, target_type: str) -> List[Node]:
    results: List[Node] = []
    if node.type == target_type:
        results.append(node)
    for child in node.children:
        results.extend(_walk(child, target_type))
    return results


def _find_string_args(node: Node) -> List[str]:
    strings: List[str] = []
    for child in _walk(node, "interpreted_string_literal"):
        strings.append(child.text.decode("utf-8").strip('"'))
    for child in _walk(node, "raw_string_literal"):
        strings.append(child.text.decode("utf-8").strip('`'))
    return strings


def _detect_framework(
    imports: Dict[str, str], tree: Node,
) -> str:
    for pattern, framework_name in _FRAMEWORK_PATTERNS.items():
        alias, method = pattern.split(".")
        if alias in imports:
            for call_node in _walk(tree, "call_expression"):
                text = call_node.text.decode("utf-8")
                if f"{alias}.{method}" in text:
                    return framework_name

    for alias, path in imports.items():
        if "google.golang.org/grpc" == path:
            for call_node in _walk(tree, "call_expression"):
                text = call_node.text.decode("utf-8")
                for pat in _GRPC_SERVER_PATTERNS:
                    if f"{alias}.{pat}" in text:
                        return "grpc"
    return ""


class GoASTExtractor:
    def __init__(self) -> None:
        self._parser = Parser(GO_LANGUAGE)

    def extract(
        self, file_path: str, content: str,
    ) -> ASTExtractionResult:
        if not file_path.endswith(".go"):
            return ASTExtractionResult()

        tree = self._parser.parse(content.encode("utf-8"))
        root = tree.root_node
        package_name = _get_package_name(root)
        imports = _find_imports(root)
        service_id = _derive_service_id(file_path)
        result = ASTExtractionResult()

        is_server = self._detect_server(root, imports)
        framework = _detect_framework(imports, root)
        otel_detected = any(
            p.startswith("go.opentelemetry.io") for p in imports.values()
        )
        if is_server:
            result.services.append(ASTServiceNode(
                service_id=service_id,
                name=package_name or service_id,
                language="go",
                framework=framework or "net/http",
                opentelemetry_enabled=otel_detected,
            ))

        self._detect_http_calls(root, imports, service_id, result)
        self._detect_kafka_usage(root, imports, result)

        return result

    def _detect_server(
        self, root: Node, imports: Dict[str, str],
    ) -> bool:
        for call_node in _walk(root, "call_expression"):
            text = call_node.text.decode("utf-8")
            for alias, path in imports.items():
                if path == "net/http":
                    for pat in _HTTP_SERVER_PATTERNS:
                        if f"{alias}.{pat}" in text:
                            return True
                if path == "google.golang.org/grpc":
                    for pat in _GRPC_SERVER_PATTERNS:
                        if f"{alias}.{pat}" in text:
                            return True

            for pattern in _FRAMEWORK_PATTERNS:
                alias_name = pattern.split(".")[0]
                if alias_name in imports and pattern in text:
                    return True

        return False

    def _detect_http_calls(
        self,
        root: Node,
        imports: Dict[str, str],
        source_id: str,
        result: ASTExtractionResult,
    ) -> None:
        for call_node in _walk(root, "call_expression"):
            text = call_node.text.decode("utf-8")
            for alias, path in imports.items():
                if path != "net/http":
                    continue
                for pat in _HTTP_CLIENT_PATTERNS:
                    if f"{alias}.{pat}" in text:
                        url_hints = _find_string_args(call_node)
                        target = url_hints[0] if url_hints else "unknown"
                        result.calls.append(ASTCallEdge(
                            source_service_id=source_id,
                            target_hint=target,
                            protocol="http",
                        ))
                        break

    def _detect_kafka_usage(
        self,
        root: Node,
        imports: Dict[str, str],
        result: ASTExtractionResult,
    ) -> None:
        has_kafka_import = any(
            "franz-go" in p or "sarama" in p for p in imports.values()
        )
        if not has_kafka_import:
            return
        for call_node in _walk(root, "call_expression"):
            text = call_node.text.decode("utf-8")
            self._collect_kafka_topics(
                text, call_node, _KAFKA_CONSUMER_FUNCS, result.topics_consumed,
            )
            self._collect_kafka_topics(
                text, call_node, _KAFKA_PRODUCER_FUNCS, result.topics_produced,
            )

    @staticmethod
    def _collect_kafka_topics(
        text: str,
        call_node: Node,
        patterns: frozenset[str],
        target_list: List[str],
    ) -> None:
        for func_name in patterns:
            if func_name in text:
                for s in _find_string_args(call_node):
                    if s not in target_list:
                        target_list.append(s)

    def extract_all(
        self, files: List[Dict[str, str]],
    ) -> ASTExtractionResult:
        combined = ASTExtractionResult()
        seen_ids: set[str] = set()

        for file_entry in files:
            path = file_entry.get("path", "")
            content = file_entry.get("content", "")
            if not path.endswith(".go"):
                continue
            single = self.extract(path, content)
            for svc in single.services:
                if svc.service_id not in seen_ids:
                    seen_ids.add(svc.service_id)
                    combined.services.append(svc)
            combined.calls.extend(single.calls)
            for topic in single.topics_consumed:
                if topic not in combined.topics_consumed:
                    combined.topics_consumed.append(topic)
            for topic in single.topics_produced:
                if topic not in combined.topics_produced:
                    combined.topics_produced.append(topic)

        return combined


_PY_FRAMEWORK_CONSTRUCTORS: Dict[str, str] = {
    "FastAPI": "fastapi",
    "Flask": "flask",
}

_PY_HTTP_MODULES = frozenset({"httpx", "requests", "aiohttp"})

_PY_HTTP_METHODS = frozenset({"get", "post", "put", "patch", "delete", "head", "options"})

_PY_KAFKA_MODULES = frozenset({"aiokafka", "kafka", "confluent_kafka"})

_PY_KAFKA_CONSUMER_CLASSES = frozenset({
    "AIOKafkaConsumer",
    "KafkaConsumer",
})


class _PythonVisitor(ast.NodeVisitor):
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self.service_id = _derive_service_id(file_path)
        self.result = ASTExtractionResult()
        self._imports: Dict[str, str] = {}
        self._from_imports: Dict[str, str] = {}

    @property
    def has_kafka_imports(self) -> bool:
        for module_path in self._imports.values():
            if module_path.split(".")[0] in _PY_KAFKA_MODULES:
                return True
        for fqn in self._from_imports.values():
            if fqn.split(".")[0] in _PY_KAFKA_MODULES:
                return True
        return False

    @property
    def has_otel_imports(self) -> bool:
        for module_path in self._imports.values():
            if module_path.split(".")[0] == "opentelemetry":
                return True
        for fqn in self._from_imports.values():
            if fqn.split(".")[0] == "opentelemetry":
                return True
        return False

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            name = alias.asname or alias.name.split(".")[-1]
            self._imports[name] = alias.name
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        for alias in node.names:
            local = alias.asname or alias.name
            self._from_imports[local] = f"{module}.{alias.name}"
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func_name = self._resolve_call_name(node)
        self._check_framework(func_name)
        self._check_grpc_server(func_name)
        self._check_http_call(func_name, node)
        self._check_kafka_consumer(func_name, node)
        self.generic_visit(node)

    def _resolve_call_name(self, node: ast.Call) -> str:
        if isinstance(node.func, ast.Name):
            return node.func.id
        if isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Name):
                return f"{node.func.value.id}.{node.func.attr}"
            return node.func.attr
        return ""

    def _check_framework(self, func_name: str) -> None:
        short = func_name.split(".")[-1] if "." in func_name else func_name
        if short in _PY_FRAMEWORK_CONSTRUCTORS:
            framework = _PY_FRAMEWORK_CONSTRUCTORS[short]
            self.result.services.append(ASTServiceNode(
                service_id=self.service_id,
                name=self.service_id,
                language="python",
                framework=framework,
            ))

    def _check_grpc_server(self, func_name: str) -> None:
        if func_name in ("grpc.server", "server"):
            source_module = self._from_imports.get("server", "")
            if func_name == "grpc.server" or "grpc" in source_module:
                self.result.services.append(ASTServiceNode(
                    service_id=self.service_id,
                    name=self.service_id,
                    language="python",
                    framework="grpc",
                ))

    def _check_http_call(self, func_name: str, node: ast.Call) -> None:
        parts = func_name.split(".")
        if len(parts) != 2:
            return
        module_alias, method = parts
        actual_module = self._imports.get(module_alias, module_alias)
        if actual_module not in _PY_HTTP_MODULES:
            return
        if method not in _PY_HTTP_METHODS:
            return
        url = self._extract_first_string_arg(node)
        self.result.calls.append(ASTCallEdge(
            source_service_id=self.service_id,
            target_hint=url,
            protocol="http",
        ))

    def _check_kafka_consumer(self, func_name: str, node: ast.Call) -> None:
        short = func_name.split(".")[-1]
        if short not in _PY_KAFKA_CONSUMER_CLASSES:
            return
        for arg in node.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                if arg.value not in self.result.topics_consumed:
                    self.result.topics_consumed.append(arg.value)

    @staticmethod
    def _extract_first_string_arg(node: ast.Call) -> str:
        for arg in node.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                return arg.value
            if isinstance(arg, ast.JoinedStr):
                parts = []
                for val in arg.values:
                    if isinstance(val, ast.Constant):
                        parts.append(str(val.value))
                    else:
                        parts.append("{...}")
                return "".join(parts)
        return "unknown"


class _KafkaSendVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.topics: List[str] = []

    def visit_Call(self, node: ast.Call) -> None:
        func_name = ""
        if isinstance(node.func, ast.Attribute):
            func_name = node.func.attr
        if func_name == "send" and node.args:
            first_arg = node.args[0]
            if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                self.topics.append(first_arg.value)
        self.generic_visit(node)


class PythonASTExtractor:
    def extract(
        self, file_path: str, content: str,
    ) -> ASTExtractionResult:
        if not file_path.endswith(".py"):
            return ASTExtractionResult()

        try:
            tree = ast.parse(content)
        except SyntaxError:
            return ASTExtractionResult()

        visitor = _PythonVisitor(file_path)
        visitor.visit(tree)

        if visitor.has_otel_imports:
            for svc in visitor.result.services:
                svc.opentelemetry_enabled = True

        if visitor.has_kafka_imports:
            send_visitor = _KafkaSendVisitor()
            send_visitor.visit(tree)
            for topic in send_visitor.topics:
                if topic not in visitor.result.topics_produced:
                    visitor.result.topics_produced.append(topic)

        return visitor.result

    def extract_all(
        self, files: List[Dict[str, str]],
    ) -> ASTExtractionResult:
        combined = ASTExtractionResult()
        seen_ids: set[str] = set()

        for file_entry in files:
            path = file_entry.get("path", "")
            content = file_entry.get("content", "")
            if not path.endswith(".py"):
                continue
            single = self.extract(path, content)
            for svc in single.services:
                if svc.service_id not in seen_ids:
                    seen_ids.add(svc.service_id)
                    combined.services.append(svc)
            combined.calls.extend(single.calls)
            for topic in single.topics_consumed:
                if topic not in combined.topics_consumed:
                    combined.topics_consumed.append(topic)
            for topic in single.topics_produced:
                if topic not in combined.topics_produced:
                    combined.topics_produced.append(topic)

        return combined
