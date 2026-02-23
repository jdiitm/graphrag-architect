import logging
import os
import time
from typing import Any, Dict, List, TypedDict

from langgraph.graph import END, START, StateGraph
from neo4j.exceptions import Neo4jError
from opentelemetry.trace import StatusCode

from orchestrator.app.ast_extraction import GoASTExtractor, PythonASTExtractor
from orchestrator.app.checkpointing import ExtractionCheckpoint, FileStatus
from orchestrator.app.config import ExtractionConfig
from orchestrator.app.extraction_models import (
    CallsEdge,
    K8sDeploymentNode,
    KafkaTopicNode,
    ServiceNode,
)
from orchestrator.app.llm_extraction import ServiceExtractor
from orchestrator.app.manifest_parser import parse_all_manifests
from orchestrator.app.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitOpenError
from orchestrator.app.neo4j_client import GraphRepository
from orchestrator.app.neo4j_pool import get_driver
from orchestrator.app.observability import (
    INGESTION_DURATION,
    LLM_EXTRACTION_DURATION,
    NEO4J_TRANSACTION_DURATION,
    get_tracer,
)
from orchestrator.app.schema_validation import validate_topology
from orchestrator.app.workspace_loader import load_directory_chunked

logger = logging.getLogger(__name__)

MAX_VALIDATION_RETRIES = 3

_NEO4J_CIRCUIT_BREAKER = CircuitBreaker(CircuitBreakerConfig())


def _build_extractor() -> ServiceExtractor:
    return ServiceExtractor(ExtractionConfig.from_env())


class IngestionState(TypedDict):
    directory_path: str
    raw_files: List[Dict[str, str]]
    extracted_nodes: List[Any]
    extraction_errors: List[str]
    validation_retries: int
    commit_status: str
    extraction_checkpoint: Dict[str, str]


def _get_workspace_max_bytes() -> int:
    return int(os.environ.get("WORKSPACE_MAX_BYTES", "104857600"))


def load_workspace_files(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.load_workspace") as span:
        start = time.monotonic()
        directory_path = state.get("directory_path", "")
        if not directory_path:
            files = state.get("raw_files", [])
            span.set_attribute("file_count", len(files))
            INGESTION_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "load_workspace"}
            )
            return {"raw_files": files}
        max_bytes = _get_workspace_max_bytes()
        files: List[Dict[str, str]] = []
        for chunk in load_directory_chunked(
            directory_path, max_total_bytes=max_bytes,
        ):
            files.extend(chunk)
        files.sort(key=lambda entry: entry["path"])
        span.set_attribute("file_count", len(files))
        INGESTION_DURATION.record(
            (time.monotonic() - start) * 1000, {"node": "load_workspace"}
        )
        return {"raw_files": files}


def parse_source_ast(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.parse_source_ast"):
        start = time.monotonic()
        raw_files = state.get("raw_files", [])
        checkpoint = _load_or_create_checkpoint(state, raw_files)
        pending = checkpoint.filter_files(raw_files, FileStatus.PENDING)
        go_extractor = GoASTExtractor()
        py_extractor = PythonASTExtractor()
        go_result = go_extractor.extract_all(pending)
        py_result = py_extractor.extract_all(pending)
        extracted_paths = (
            [f["path"] for f in pending if f["path"].endswith(".go")]
            + [f["path"] for f in pending if f["path"].endswith(".py")]
        )
        checkpoint.mark(extracted_paths, FileStatus.EXTRACTED)
        nodes: List[Any] = []
        for ast_svc in go_result.services + py_result.services:
            nodes.append(ServiceNode(
                id=ast_svc.service_id,
                name=ast_svc.name,
                language=ast_svc.language,
                framework=ast_svc.framework,
                opentelemetry_enabled=False,
                confidence=1.0,
            ))
        for ast_call in go_result.calls + py_result.calls:
            nodes.append(CallsEdge(
                source_service_id=ast_call.source_service_id,
                target_service_id=ast_call.target_hint,
                protocol=ast_call.protocol,
                confidence=1.0,
            ))
        INGESTION_DURATION.record(
            (time.monotonic() - start) * 1000, {"node": "parse_source_ast"}
        )
        return {
            "extracted_nodes": nodes,
            "extraction_checkpoint": checkpoint.to_dict(),
        }


def _load_or_create_checkpoint(
    state: IngestionState, raw_files: List[Dict[str, str]],
) -> ExtractionCheckpoint:
    existing = state.get("extraction_checkpoint")
    if existing:
        return ExtractionCheckpoint.from_dict(existing)
    return ExtractionCheckpoint.from_files(raw_files)


async def enrich_with_llm(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.enrich_with_llm"):
        start = time.monotonic()
        existing = list(state.get("extracted_nodes", []))
        try:
            extractor = _build_extractor()
            result = await extractor.extract_all(state["raw_files"])
            for svc in result.services:
                svc.confidence = 0.7
                if not any(
                    isinstance(n, ServiceNode) and n.id == svc.id
                    for n in existing
                ):
                    existing.append(svc)
            for call in result.calls:
                call.confidence = 0.7
                existing.append(call)
        except Exception:
            logger.warning("LLM enrichment unavailable, using AST results only")
        elapsed_ms = (time.monotonic() - start) * 1000
        LLM_EXTRACTION_DURATION.record(elapsed_ms, {"node": "enrich_with_llm"})
        return {"extracted_nodes": existing}

def parse_k8s_and_kafka_manifests(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.parse_manifests"):
        start = time.monotonic()
        existing = list(state.get("extracted_nodes", []))
        manifest_entities = parse_all_manifests(state.get("raw_files", []))
        INGESTION_DURATION.record(
            (time.monotonic() - start) * 1000, {"node": "parse_manifests"}
        )
        return {"extracted_nodes": existing + manifest_entities}

def validate_extracted_schema(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.validate_schema"):
        errors = validate_topology(state.get("extracted_nodes", []))
        return {"extraction_errors": errors}

def route_validation(state: IngestionState) -> str:
    if state.get("extraction_errors"):
        if state.get("validation_retries", 0) >= MAX_VALIDATION_RETRIES:
            return "commit_to_neo4j"
        return "fix_extraction_errors"
    return "commit_to_neo4j"

def _extract_manifest_entities(nodes: List[Any]) -> List[Any]:
    return [
        n for n in nodes
        if isinstance(n, (K8sDeploymentNode, KafkaTopicNode))
    ]


async def fix_extraction_errors(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.fix_errors"):
        start = time.monotonic()
        raw_files = state.get("raw_files", [])
        checkpoint = _load_or_create_checkpoint(state, raw_files)
        checkpoint.retry_failed()
        failed_files = checkpoint.filter_files(raw_files, FileStatus.PENDING)
        extractor = _build_extractor()
        result = await extractor.extract_all(failed_files)
        retry_paths = [f["path"] for f in failed_files]
        checkpoint.mark(retry_paths, FileStatus.EXTRACTED)
        retries = state.get("validation_retries", 0)
        manifest_entities = _extract_manifest_entities(
            state.get("extracted_nodes", [])
        )
        LLM_EXTRACTION_DURATION.record(
            (time.monotonic() - start) * 1000, {"node": "fix_errors"}
        )
        return {
            "extracted_nodes": (
                list(result.services) + list(result.calls) + manifest_entities
            ),
            "extraction_errors": [],
            "validation_retries": retries + 1,
            "extraction_checkpoint": checkpoint.to_dict(),
        }

async def commit_to_neo4j(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.commit_neo4j") as span:
        start = time.monotonic()
        try:
            driver = get_driver()
            repo = GraphRepository(driver, circuit_breaker=_NEO4J_CIRCUIT_BREAKER)
            await repo.commit_topology(state.get("extracted_nodes", []))
            return {"commit_status": "success"}
        except (Neo4jError, OSError, CircuitOpenError, RuntimeError) as exc:
            span.set_status(StatusCode.ERROR, str(exc))
            span.record_exception(exc)
            logger.error("Neo4j commit failed: %s", exc)
            return {"commit_status": "failed"}
        finally:
            NEO4J_TRANSACTION_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "commit_neo4j"}
            )

builder = StateGraph(IngestionState)

builder.add_node("load_workspace", load_workspace_files)
builder.add_node("parse_source_ast", parse_source_ast)
builder.add_node("enrich_with_llm", enrich_with_llm)
builder.add_node("parse_manifests", parse_k8s_and_kafka_manifests)
builder.add_node("validate_schema", validate_extracted_schema)
builder.add_node("fix_errors", fix_extraction_errors)
builder.add_node("commit_graph", commit_to_neo4j)

builder.add_edge(START, "load_workspace")
builder.add_edge("load_workspace", "parse_source_ast")
builder.add_edge("parse_source_ast", "enrich_with_llm")
builder.add_edge("enrich_with_llm", "parse_manifests")
builder.add_edge("parse_manifests", "validate_schema")

builder.add_conditional_edges(
    "validate_schema",
    route_validation,
    {
        "fix_extraction_errors": "fix_errors",
        "commit_to_neo4j": "commit_graph"
    }
)

builder.add_edge("fix_errors", "validate_schema")
builder.add_edge("commit_graph", END)

ingestion_graph = builder.compile()
