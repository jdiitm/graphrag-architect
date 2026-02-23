from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from orchestrator.app.ast_extraction import GoASTExtractor, PythonASTExtractor
from orchestrator.app.extraction_models import (
    CallsEdge,
    ServiceExtractionResult,
    ServiceNode,
)


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


class TestConfidenceBounds:
    def test_service_node_rejects_confidence_above_one(self):
        with pytest.raises(ValidationError):
            ServiceNode(
                id="auth", name="auth", language="go",
                framework="gin", opentelemetry_enabled=True, confidence=1.5,
            )

    def test_service_node_rejects_negative_confidence(self):
        with pytest.raises(ValidationError):
            ServiceNode(
                id="auth", name="auth", language="go",
                framework="gin", opentelemetry_enabled=True, confidence=-0.1,
            )

    def test_calls_edge_rejects_confidence_above_one(self):
        with pytest.raises(ValidationError):
            CallsEdge(
                source_service_id="auth", target_service_id="orders",
                protocol="http", confidence=2.0,
            )

    def test_calls_edge_rejects_negative_confidence(self):
        with pytest.raises(ValidationError):
            CallsEdge(
                source_service_id="auth", target_service_id="orders",
                protocol="http", confidence=-0.5,
            )


class TestCypherIncludesConfidence:
    def test_service_cypher_sets_confidence(self):
        from orchestrator.app.neo4j_client import _service_cypher

        node = ServiceNode(
            id="auth", name="auth", language="go",
            framework="gin", opentelemetry_enabled=True, confidence=0.85,
        )
        query, params = _service_cypher(node)
        assert "confidence" in query
        assert params["confidence"] == 0.85

    def test_calls_cypher_sets_confidence(self):
        from orchestrator.app.neo4j_client import _calls_cypher

        edge = CallsEdge(
            source_service_id="auth", target_service_id="orders",
            protocol="http", confidence=0.7,
        )
        query, params = _calls_cypher(edge)
        assert "confidence" in query
        assert params["confidence"] == 0.7


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


class TestFixExtractionErrorsPreservesAST:
    @pytest.mark.asyncio
    async def test_ast_entities_survive_retry(self):
        from orchestrator.app.graph_builder import fix_extraction_errors

        ast_service = ServiceNode(
            id="auth", name="auth", language="go",
            framework="gin", opentelemetry_enabled=False, confidence=1.0,
        )
        ast_edge = CallsEdge(
            source_service_id="auth", target_service_id="orders",
            protocol="http", confidence=1.0,
        )
        llm_service = ServiceNode(
            id="payments", name="payments", language="python",
            framework="fastapi", opentelemetry_enabled=False, confidence=0.7,
        )
        state = {
            "directory_path": "",
            "raw_files": [{"path": "main.go", "content": "package main"}],
            "extracted_nodes": [ast_service, ast_edge, llm_service],
            "extraction_errors": ["some error"],
            "validation_retries": 0,
            "commit_status": "",
        }

        mock_result = ServiceExtractionResult(
            services=[ServiceNode(
                id="new-svc", name="new-svc", language="go",
                framework="gin", opentelemetry_enabled=False,
            )],
            calls=[],
        )
        mock_extractor = AsyncMock()
        mock_extractor.extract_all.return_value = mock_result

        with patch(
            "orchestrator.app.graph_builder._build_extractor",
            return_value=mock_extractor,
        ):
            result = await fix_extraction_errors(state)

        nodes = result["extracted_nodes"]
        service_ids = {
            n.id for n in nodes if isinstance(n, ServiceNode)
        }
        ast_edges = [
            n for n in nodes
            if isinstance(n, CallsEdge) and n.confidence == 1.0
        ]
        assert "auth" in service_ids, "AST service must survive retry"
        assert len(ast_edges) >= 1, "AST edges must survive retry"

    @pytest.mark.asyncio
    async def test_llm_only_entities_replaced_on_retry(self):
        from orchestrator.app.graph_builder import fix_extraction_errors

        ast_service = ServiceNode(
            id="auth", name="auth", language="go",
            framework="gin", opentelemetry_enabled=False, confidence=1.0,
        )
        stale_llm = ServiceNode(
            id="stale-llm", name="stale", language="python",
            framework="flask", opentelemetry_enabled=False, confidence=0.7,
        )
        state = {
            "directory_path": "",
            "raw_files": [{"path": "main.go", "content": "package main"}],
            "extracted_nodes": [ast_service, stale_llm],
            "extraction_errors": ["some error"],
            "validation_retries": 0,
            "commit_status": "",
        }

        fresh_llm = ServiceNode(
            id="fresh-llm", name="fresh", language="go",
            framework="gin", opentelemetry_enabled=False,
        )
        mock_result = ServiceExtractionResult(
            services=[fresh_llm], calls=[],
        )
        mock_extractor = AsyncMock()
        mock_extractor.extract_all.return_value = mock_result

        with patch(
            "orchestrator.app.graph_builder._build_extractor",
            return_value=mock_extractor,
        ):
            result = await fix_extraction_errors(state)

        service_ids = {
            n.id for n in result["extracted_nodes"]
            if isinstance(n, ServiceNode)
        }
        assert "stale-llm" not in service_ids, "Stale LLM entity must be replaced"
        assert "fresh-llm" in service_ids, "Fresh LLM entity must appear"
        assert "auth" in service_ids, "AST entity must survive"


class TestGracefulDegradation:
    @pytest.mark.asyncio
    async def test_llm_failure_preserves_ast_entities(self):
        from orchestrator.app.graph_builder import enrich_with_llm

        ast_service = ServiceNode(
            id="auth", name="auth", language="go",
            framework="gin", opentelemetry_enabled=False, confidence=1.0,
        )
        state = {
            "directory_path": "",
            "raw_files": [{"path": "main.go", "content": "package main"}],
            "extracted_nodes": [ast_service],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }

        mock_extractor = AsyncMock()
        mock_extractor.extract_all.side_effect = OSError("LLM unavailable")

        with patch(
            "orchestrator.app.graph_builder._build_extractor",
            return_value=mock_extractor,
        ):
            result = await enrich_with_llm(state)

        services = [
            n for n in result["extracted_nodes"]
            if isinstance(n, ServiceNode)
        ]
        assert len(services) == 1
        assert services[0].id == "auth"
        assert services[0].confidence == 1.0


class TestEdgeDeduplication:
    @pytest.mark.asyncio
    async def test_duplicate_edge_keeps_ast_version(self):
        from orchestrator.app.graph_builder import enrich_with_llm

        ast_edge = CallsEdge(
            source_service_id="auth", target_service_id="orders",
            protocol="http", confidence=1.0,
        )
        state = {
            "directory_path": "",
            "raw_files": [{"path": "main.go", "content": "package main"}],
            "extracted_nodes": [ast_edge],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }

        llm_edge = CallsEdge(
            source_service_id="auth", target_service_id="orders",
            protocol="http", confidence=0.7,
        )
        mock_result = ServiceExtractionResult(services=[], calls=[llm_edge])
        mock_extractor = AsyncMock()
        mock_extractor.extract_all.return_value = mock_result

        with patch(
            "orchestrator.app.graph_builder._build_extractor",
            return_value=mock_extractor,
        ):
            result = await enrich_with_llm(state)

        edges = [
            n for n in result["extracted_nodes"]
            if isinstance(n, CallsEdge)
        ]
        assert len(edges) == 1, f"Expected 1 edge, got {len(edges)}"
        assert edges[0].confidence == 1.0

    @pytest.mark.asyncio
    async def test_unique_llm_edge_is_added(self):
        from orchestrator.app.graph_builder import enrich_with_llm

        ast_edge = CallsEdge(
            source_service_id="auth", target_service_id="orders",
            protocol="http", confidence=1.0,
        )
        state = {
            "directory_path": "",
            "raw_files": [{"path": "main.go", "content": "package main"}],
            "extracted_nodes": [ast_edge],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }

        llm_edge = CallsEdge(
            source_service_id="auth", target_service_id="payments",
            protocol="grpc", confidence=0.7,
        )
        mock_result = ServiceExtractionResult(services=[], calls=[llm_edge])
        mock_extractor = AsyncMock()
        mock_extractor.extract_all.return_value = mock_result

        with patch(
            "orchestrator.app.graph_builder._build_extractor",
            return_value=mock_extractor,
        ):
            result = await enrich_with_llm(state)

        edges = [
            n for n in result["extracted_nodes"]
            if isinstance(n, CallsEdge)
        ]
        assert len(edges) == 2


class TestOTelDetection:
    def test_go_ast_detects_otel_imports(self):
        go_source = '''package main

import (
    "net/http"
    "go.opentelemetry.io/otel"
)

func main() {
    otel.Tracer("myapp")
    http.ListenAndServe(":8080", nil)
}
'''
        extractor = GoASTExtractor()
        result = extractor.extract("cmd/main.go", go_source)
        assert len(result.services) == 1
        assert result.services[0].opentelemetry_enabled is True

    def test_go_ast_no_otel_when_not_imported(self):
        go_source = '''package main

import "net/http"

func main() {
    http.ListenAndServe(":8080", nil)
}
'''
        extractor = GoASTExtractor()
        result = extractor.extract("cmd/main.go", go_source)
        assert len(result.services) == 1
        assert result.services[0].opentelemetry_enabled is False

    def test_python_ast_detects_otel_imports(self):
        py_source = '''
from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

app = FastAPI()
FastAPIInstrumentor.instrument_app(app)
'''
        extractor = PythonASTExtractor()
        result = extractor.extract("svc/app.py", py_source)
        assert len(result.services) == 1
        assert result.services[0].opentelemetry_enabled is True

    def test_python_ast_no_otel_when_not_imported(self):
        py_source = '''
from fastapi import FastAPI
app = FastAPI()
'''
        extractor = PythonASTExtractor()
        result = extractor.extract("svc/app.py", py_source)
        assert len(result.services) == 1
        assert result.services[0].opentelemetry_enabled is False

    def test_parse_source_ast_passes_otel_through(self):
        from orchestrator.app.graph_builder import parse_source_ast

        go_source = '''package main

import (
    "net/http"
    "go.opentelemetry.io/otel"
)

func main() {
    otel.Tracer("x")
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
        services = [
            n for n in result["extracted_nodes"]
            if isinstance(n, ServiceNode)
        ]
        assert len(services) >= 1
        assert services[0].opentelemetry_enabled is True


class TestEnrichMergesOTel:
    @pytest.mark.asyncio
    async def test_llm_otel_merged_into_ast_node(self):
        from orchestrator.app.graph_builder import enrich_with_llm

        ast_service = ServiceNode(
            id="auth", name="auth", language="go",
            framework="gin", opentelemetry_enabled=False, confidence=1.0,
        )
        state = {
            "directory_path": "",
            "raw_files": [{"path": "main.go", "content": "package main"}],
            "extracted_nodes": [ast_service],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }

        llm_service = ServiceNode(
            id="auth", name="auth-service", language="go",
            framework="gin", opentelemetry_enabled=True,
        )
        mock_result = ServiceExtractionResult(
            services=[llm_service], calls=[],
        )
        mock_extractor = AsyncMock()
        mock_extractor.extract_all.return_value = mock_result

        with patch(
            "orchestrator.app.graph_builder._build_extractor",
            return_value=mock_extractor,
        ):
            result = await enrich_with_llm(state)

        services = [
            n for n in result["extracted_nodes"]
            if isinstance(n, ServiceNode)
        ]
        auth_svc = next(s for s in services if s.id == "auth")
        assert auth_svc.opentelemetry_enabled is True
        assert auth_svc.confidence == 1.0


class TestDAGWiring:
    def test_ingestion_graph_has_parse_source_ast_node(self):
        from orchestrator.app.graph_builder import ingestion_graph
        node_names = set(ingestion_graph.get_graph().nodes.keys())
        assert "parse_source_ast" in node_names

    def test_ingestion_graph_has_enrich_with_llm_node(self):
        from orchestrator.app.graph_builder import ingestion_graph
        node_names = set(ingestion_graph.get_graph().nodes.keys())
        assert "enrich_with_llm" in node_names
