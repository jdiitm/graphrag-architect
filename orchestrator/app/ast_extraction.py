from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List

import tree_sitter_go
from tree_sitter import Language, Parser, Node


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

_GRPC_CLIENT_PATTERNS = frozenset({
    "Dial",
    "DialContext",
    "NewClient",
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


@dataclass
class ASTCallEdge:
    source_service_id: str
    target_hint: str
    protocol: str


@dataclass
class ASTExtractionResult:
    services: List[ASTServiceNode] = field(default_factory=list)
    calls: List[ASTCallEdge] = field(default_factory=list)
    topics_consumed: List[str] = field(default_factory=list)
    topics_produced: List[str] = field(default_factory=list)


def _derive_service_id(file_path: str) -> str:
    parts = file_path.replace("\\", "/").split("/")
    if len(parts) >= 2:
        return parts[-2]
    name = os.path.splitext(parts[-1])[0]
    return name


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
        if is_server:
            result.services.append(ASTServiceNode(
                service_id=service_id,
                name=package_name or service_id,
                language="go",
                framework=framework or "net/http",
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
                        return

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
