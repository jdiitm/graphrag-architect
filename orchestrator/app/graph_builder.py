import time
from typing import Any, Dict, List, TypedDict

from langgraph.graph import END, START, StateGraph
from neo4j.exceptions import Neo4jError

from orchestrator.app.config import ExtractionConfig
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
from orchestrator.app.workspace_loader import load_directory


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
        files = load_directory(directory_path)
        span.set_attribute("file_count", len(files))
        INGESTION_DURATION.record(
            (time.monotonic() - start) * 1000, {"node": "load_workspace"}
        )
        return {"raw_files": files}


async def parse_go_and_python_services(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.parse_services"):
        start = time.monotonic()
        extractor = _build_extractor()
        result = await extractor.extract_all(state["raw_files"])
        elapsed_ms = (time.monotonic() - start) * 1000
        LLM_EXTRACTION_DURATION.record(elapsed_ms, {"node": "parse_services"})
        return {"extracted_nodes": list(result.services) + list(result.calls)}

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

async def fix_extraction_errors(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.fix_errors"):
        start = time.monotonic()
        extractor = _build_extractor()
        result = await extractor.extract_all(state.get("raw_files", []))
        retries = state.get("validation_retries", 0)
        LLM_EXTRACTION_DURATION.record(
            (time.monotonic() - start) * 1000, {"node": "fix_errors"}
        )
        return {
            "extracted_nodes": list(result.services) + list(result.calls),
            "extraction_errors": [],
            "validation_retries": retries + 1,
        }

async def commit_to_neo4j(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.commit_neo4j"):
        start = time.monotonic()
        try:
            driver = get_driver()
            repo = GraphRepository(driver, circuit_breaker=_NEO4J_CIRCUIT_BREAKER)
            await repo.commit_topology(state.get("extracted_nodes", []))
            return {"commit_status": "success"}
        except (Neo4jError, OSError, CircuitOpenError, RuntimeError):
            return {"commit_status": "failed"}
        finally:
            NEO4J_TRANSACTION_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "commit_neo4j"}
            )

builder = StateGraph(IngestionState)

builder.add_node("load_workspace", load_workspace_files)
builder.add_node("parse_services", parse_go_and_python_services)
builder.add_node("parse_manifests", parse_k8s_and_kafka_manifests)
builder.add_node("validate_schema", validate_extracted_schema)
builder.add_node("fix_errors", fix_extraction_errors)
builder.add_node("commit_graph", commit_to_neo4j)

builder.add_edge(START, "load_workspace")
builder.add_edge("load_workspace", "parse_services")
builder.add_edge("parse_services", "parse_manifests")
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
