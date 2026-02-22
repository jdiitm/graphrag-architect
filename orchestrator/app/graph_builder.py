from typing import Any, Dict, List, TypedDict

from langgraph.graph import END, START, StateGraph
from neo4j import AsyncGraphDatabase
from neo4j.exceptions import Neo4jError

from orchestrator.app.config import ExtractionConfig, Neo4jConfig
from orchestrator.app.llm_extraction import ServiceExtractor
from orchestrator.app.neo4j_client import GraphRepository
from orchestrator.app.schema_validation import validate_topology
from orchestrator.app.workspace_loader import load_directory


MAX_VALIDATION_RETRIES = 3


class IngestionState(TypedDict):
    directory_path: str
    raw_files: List[Dict[str, str]]
    extracted_nodes: List[Any]
    extraction_errors: List[str]
    validation_retries: int
    commit_status: str


def load_workspace_files(state: IngestionState) -> dict:
    directory_path = state.get("directory_path", "")
    return {"raw_files": load_directory(directory_path)}


async def parse_go_and_python_services(state: IngestionState) -> dict:
    config = ExtractionConfig.from_env()
    extractor = ServiceExtractor(config)
    result = await extractor.extract_all(state["raw_files"])
    return {"extracted_nodes": list(result.services) + list(result.calls)}

def parse_k8s_and_kafka_manifests(state: IngestionState) -> dict:
    return {"extracted_nodes": []}

def validate_extracted_schema(state: IngestionState) -> dict:
    errors = validate_topology(state.get("extracted_nodes", []))
    return {"extraction_errors": errors}

def route_validation(state: IngestionState) -> str:
    if state.get("extraction_errors"):
        if state.get("validation_retries", 0) >= MAX_VALIDATION_RETRIES:
            return "commit_to_neo4j"
        return "fix_extraction_errors"
    return "commit_to_neo4j"

async def fix_extraction_errors(state: IngestionState) -> dict:
    config = ExtractionConfig.from_env()
    extractor = ServiceExtractor(config)
    result = await extractor.extract_all(state.get("raw_files", []))
    retries = state.get("validation_retries", 0)
    return {
        "extracted_nodes": list(result.services) + list(result.calls),
        "extraction_errors": [],
        "validation_retries": retries + 1,
    }

async def commit_to_neo4j(state: IngestionState) -> dict:
    config = Neo4jConfig.from_env()
    driver = AsyncGraphDatabase.driver(
        config.uri, auth=(config.username, config.password)
    )
    try:
        repo = GraphRepository(driver)
        await repo.commit_topology(state.get("extracted_nodes", []))
        return {"commit_status": "success"}
    except (Neo4jError, OSError):
        return {"commit_status": "failed"}
    finally:
        await driver.close()

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
