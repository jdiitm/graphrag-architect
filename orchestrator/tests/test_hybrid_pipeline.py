from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.extraction_models import CallsEdge, ServiceNode


class TestConfidenceField:
    def test_calls_edge_has_confidence_defaulting_to_one(self):
        edge = CallsEdge(
            source_service_id="auth",
            target_service_id="orders",
            protocol="http",
        )
        assert edge.confidence == 1.0

    def test_calls_edge_accepts_custom_confidence(self):
        edge = CallsEdge(
            source_service_id="auth",
            target_service_id="orders",
            protocol="http",
            confidence=0.7,
        )
        assert edge.confidence == 0.7

    def test_service_node_has_confidence(self):
        node = ServiceNode(
            id="auth",
            name="auth-service",
            language="Go",
            framework="gin",
            opentelemetry_enabled=True,
        )
        assert node.confidence == 1.0


class TestParseSourceASTNode:
    def test_ast_node_extracts_go_services(self):
        from orchestrator.app.graph_builder import parse_source_ast

        go_source = '''package main

import "net/http"

func main() {
    http.ListenAndServe(":8080", nil)
}
'''
        state = {
            "directory_path": "",
            "raw_files": [{"path": "cmd/main.go", "content": go_source}],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        result = parse_source_ast(state)
        services = [n for n in result["extracted_nodes"] if isinstance(n, ServiceNode)]
        assert len(services) >= 1
        assert services[0].confidence == 1.0

    def test_ast_node_extracts_python_services(self):
        from orchestrator.app.graph_builder import parse_source_ast

        py_source = '''
from fastapi import FastAPI
app = FastAPI()
'''
        state = {
            "directory_path": "",
            "raw_files": [{"path": "app.py", "content": py_source}],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        result = parse_source_ast(state)
        services = [n for n in result["extracted_nodes"] if isinstance(n, ServiceNode)]
        assert len(services) >= 1
        assert services[0].language == "python"

    def test_ast_node_produces_calls_with_confidence_one(self):
        from orchestrator.app.graph_builder import parse_source_ast

        go_source = '''package processor

import (
    "bytes"
    "context"
    "net/http"
)

type FP struct {
    url string
    client *http.Client
}

func (f *FP) Process(ctx context.Context) error {
    req, _ := http.NewRequestWithContext(ctx, http.MethodPost, f.url+"/ingest", bytes.NewReader(nil))
    _, err := f.client.Do(req)
    return err
}
'''
        state = {
            "directory_path": "",
            "raw_files": [{"path": "internal/processor/forwarding.go", "content": go_source}],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        result = parse_source_ast(state)
        calls = [n for n in result["extracted_nodes"] if isinstance(n, CallsEdge)]
        assert len(calls) >= 1
        assert all(c.confidence == 1.0 for c in calls)


class TestDAGWiring:
    def test_ingestion_graph_has_parse_source_ast_node(self):
        from orchestrator.app.graph_builder import ingestion_graph
        node_names = set(ingestion_graph.get_graph().nodes.keys())
        assert "parse_source_ast" in node_names

    def test_ingestion_graph_has_enrich_with_llm_node(self):
        from orchestrator.app.graph_builder import ingestion_graph
        node_names = set(ingestion_graph.get_graph().nodes.keys())
        assert "enrich_with_llm" in node_names
