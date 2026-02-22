from typing import Any, Dict, List, TypedDict

from langgraph.graph import END, START, StateGraph
from neo4j import AsyncGraphDatabase
from neo4j.exceptions import Neo4jError

from orchestrator.app.config import ExtractionConfig, Neo4jConfig
from orchestrator.app.llm_extraction import ServiceExtractor
from orchestrator.app.neo4j_client import GraphRepository


class IngestionState(TypedDict):
    directory_path: str
    raw_files: List[Dict[str, str]]
    extracted_nodes: List[Any]
    extraction_errors: List[str]
    commit_status: str


def load_workspace_files(state: IngestionState) -> dict:
    return {"raw_files": []}


async def parse_go_and_python_services(state: IngestionState) -> dict:
    config = ExtractionConfig.from_env()
    extractor = ServiceExtractor(config)
    result = await extractor.extract_all(state["raw_files"])
    return {"extracted_nodes": list(result.services) + list(result.calls)}

def parse_k8s_and_kafka_manifests(state: IngestionState) -> dict:
    return {"extracted_nodes": []}

def validate_extracted_schema(state: IngestionState) -> dict:
    return {"extraction_errors": []}

def route_validation(state: IngestionState) -> str:
    if state.get("extraction_errors"):
        return "fix_extraction_errors"
    return "commit_to_neo4j"

def fix_extraction_errors(state: IngestionState) -> dict:
    return {"extracted_nodes": [], "extraction_errors": []}

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
