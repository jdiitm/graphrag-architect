import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, TypedDict

from langgraph.graph import END, START, StateGraph
from neo4j.exceptions import Neo4jError
from opentelemetry.trace import StatusCode

from orchestrator.app.ast_extraction import GoASTExtractor, PythonASTExtractor
from orchestrator.app.checkpointing import ExtractionCheckpoint, FileStatus
from orchestrator.app.config import ExtractionConfig
from orchestrator.app.container import AppContainer
from orchestrator.app.extraction_models import (
    CallsEdge,
    ConsumesEdge,
    DeployedInEdge,
    K8sDeploymentNode,
    KafkaTopicNode,
    ProducesEdge,
    ServiceNode,
)
from orchestrator.app.llm_extraction import ServiceExtractor
from orchestrator.app.manifest_parser import parse_all_manifests
from orchestrator.app.circuit_breaker import CircuitOpenError
from orchestrator.app.entity_resolver import EntityResolver
from orchestrator.app.neo4j_client import GraphRepository
from orchestrator.app.neo4j_pool import get_driver
from orchestrator.app.observability import (
    INGESTION_DURATION,
    LLM_EXTRACTION_DURATION,
    NEO4J_TRANSACTION_DURATION,
    get_tracer,
)
from orchestrator.app.schema_validation import validate_topology
from orchestrator.app.checkpoint_store import get_checkpointer
from orchestrator.app.node_sink import IncrementalNodeSink
from orchestrator.app.workspace_loader import load_directory_chunked

SINK_BATCH_SIZE = 500

logger = logging.getLogger(__name__)

MAX_VALIDATION_RETRIES = 3


class _ContainerHolder:
    value: AppContainer | None = None


def set_container(container: AppContainer | None) -> None:
    _ContainerHolder.value = container


def get_container() -> AppContainer:
    if _ContainerHolder.value is None:
        _ContainerHolder.value = AppContainer.from_env()
    return _ContainerHolder.value


def _build_extractor() -> ServiceExtractor:
    return ServiceExtractor(ExtractionConfig.from_env())


class IngestionState(TypedDict, total=False):
    directory_path: str
    raw_files: List[Dict[str, str]]
    extracted_nodes: List[Any]
    extraction_errors: List[str]
    validation_retries: int
    commit_status: str
    extraction_checkpoint: Dict[str, str]
    skipped_files: List[str]


def _get_workspace_max_bytes() -> int:
    raw = os.environ.get("WORKSPACE_MAX_BYTES", "104857600")
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"WORKSPACE_MAX_BYTES must be a positive integer, got: {raw!r}"
        ) from exc
    if value <= 0:
        raise ValueError(
            f"WORKSPACE_MAX_BYTES must be a positive integer, got: {value}"
        )
    return value


def _load_files_sync(
    directory_path: str, max_bytes: int,
) -> Tuple[List[Dict[str, str]], List[str]]:
    skipped: List[str] = []
    files: List[Dict[str, str]] = []
    for chunk in load_directory_chunked(
        directory_path, max_total_bytes=max_bytes, skipped=skipped,
    ):
        files.extend(chunk)
    files.sort(key=lambda entry: entry["path"])
    return files, skipped


async def load_workspace_files(state: IngestionState) -> dict:
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
        files, skipped = await asyncio.to_thread(
            _load_files_sync, directory_path, max_bytes,
        )
        span.set_attribute("file_count", len(files))
        if skipped:
            span.set_attribute("skipped_count", len(skipped))
            logger.warning(
                "Skipped %d file(s) beyond workspace byte limit: %s",
                len(skipped),
                ", ".join(skipped[:10]),
            )
        INGESTION_DURATION.record(
            (time.monotonic() - start) * 1000, {"node": "load_workspace"}
        )
        return {"raw_files": files, "skipped_files": skipped}


def _run_ast_extraction(
    pending: List[Dict[str, str]],
) -> Tuple[Any, Any]:
    go_extractor = GoASTExtractor()
    py_extractor = PythonASTExtractor()
    return go_extractor.extract_all(pending), py_extractor.extract_all(pending)


async def parse_source_ast(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.parse_source_ast"):
        start = time.monotonic()
        raw_files = state.get("raw_files", [])
        checkpoint = _load_or_create_checkpoint(state, raw_files)
        pending = checkpoint.filter_files(raw_files, FileStatus.PENDING)
        go_result, py_result = await asyncio.to_thread(
            _run_ast_extraction, pending,
        )
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
                opentelemetry_enabled=ast_svc.opentelemetry_enabled,
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
                existing_svc = next(
                    (n for n in existing
                     if isinstance(n, ServiceNode) and n.id == svc.id),
                    None,
                )
                if existing_svc is not None:
                    if svc.opentelemetry_enabled:
                        existing_svc.opentelemetry_enabled = True
                else:
                    existing.append(svc)
            existing_edge_keys = {
                (e.source_service_id, e.target_service_id, e.protocol)
                for e in existing if isinstance(e, CallsEdge)
            }
            for call in result.calls:
                call.confidence = 0.7
                key = (call.source_service_id, call.target_service_id,
                       call.protocol)
                if key not in existing_edge_keys:
                    existing.append(call)
                    existing_edge_keys.add(key)
        except Exception:
            logger.warning("LLM enrichment unavailable, using AST results only")
        elapsed_ms = (time.monotonic() - start) * 1000
        LLM_EXTRACTION_DURATION.record(elapsed_ms, {"node": "enrich_with_llm"})
        return {"extracted_nodes": existing}

async def parse_k8s_and_kafka_manifests(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.parse_manifests"):
        start = time.monotonic()
        raw_files = state.get("raw_files", [])
        existing = list(state.get("extracted_nodes", []))
        manifest_entities = await asyncio.to_thread(
            parse_all_manifests, raw_files,
        )
        checkpoint = _load_or_create_checkpoint(state, raw_files)
        yaml_paths = [
            f["path"] for f in raw_files
            if f["path"].endswith((".yaml", ".yml"))
        ]
        checkpoint.mark(yaml_paths, FileStatus.EXTRACTED)
        INGESTION_DURATION.record(
            (time.monotonic() - start) * 1000, {"node": "parse_manifests"}
        )
        return {
            "extracted_nodes": existing + manifest_entities,
            "extraction_checkpoint": checkpoint.to_dict(),
        }

async def validate_extracted_schema(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.validate_schema"):
        errors = await asyncio.to_thread(
            validate_topology, state.get("extracted_nodes", []),
        )
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


def _dedup_llm_against_ast(
    ast_entities: List[Any],
    llm_result: Any,
) -> Tuple[List[Any], List[Any]]:
    ast_service_ids = {
        n.id for n in ast_entities if isinstance(n, ServiceNode)
    }
    ast_edge_keys = {
        (e.source_service_id, e.target_service_id, e.protocol)
        for e in ast_entities if isinstance(e, CallsEdge)
    }
    services = [s for s in llm_result.services if s.id not in ast_service_ids]
    calls = [
        c for c in llm_result.calls
        if (c.source_service_id, c.target_service_id, c.protocol)
        not in ast_edge_keys
    ]
    return services, calls


async def fix_extraction_errors(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.fix_errors"):
        start = time.monotonic()
        existing = state.get("extracted_nodes", [])
        raw_files = state.get("raw_files", [])
        checkpoint = _load_or_create_checkpoint(state, raw_files)
        checkpoint.retry_failed()
        failed_files = checkpoint.filter_files(raw_files, FileStatus.PENDING)
        ast_entities = [
            n for n in existing
            if getattr(n, "confidence", 0) == 1.0
        ]
        manifest_entities = _extract_manifest_entities(existing)

        extractor = _build_extractor()
        result = await extractor.extract_all(failed_files)
        checkpoint.mark([f["path"] for f in failed_files], FileStatus.EXTRACTED)
        llm_services, llm_calls = _dedup_llm_against_ast(ast_entities, result)

        LLM_EXTRACTION_DURATION.record(
            (time.monotonic() - start) * 1000, {"node": "fix_errors"}
        )
        return {
            "extracted_nodes": (
                ast_entities + llm_services + llm_calls + manifest_entities
            ),
            "extraction_errors": [],
            "validation_retries": state.get("validation_retries", 0) + 1,
            "extraction_checkpoint": checkpoint.to_dict(),
        }

_EDGE_TYPES = (CallsEdge, ProducesEdge, ConsumesEdge, DeployedInEdge)


def _stamp_ingestion_metadata(
    entities: List[Any], ingestion_id: str,
) -> List[Any]:
    edge_types = _EDGE_TYPES
    now_iso = datetime.now(timezone.utc).isoformat()
    for entity in entities:
        if isinstance(entity, edge_types):
            entity.ingestion_id = ingestion_id
            entity.last_seen_at = now_iso
    return entities


async def commit_to_neo4j(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.commit_neo4j") as span:
        start = time.monotonic()
        try:
            entities = state.get("extracted_nodes", [])
            resolver = EntityResolver()
            entities = resolver.resolve_entities(entities)
            ingestion_id = str(uuid.uuid4())
            _stamp_ingestion_metadata(entities, ingestion_id)
            driver = get_driver()
            repo = GraphRepository(
                driver, circuit_breaker=get_container().circuit_breaker
            )
            sink = IncrementalNodeSink(repo, batch_size=SINK_BATCH_SIZE)
            await sink.ingest(entities)
            await sink.flush()
            span.set_attribute("entity_count", sink.total_entities)
            span.set_attribute("flush_count", sink.flush_count)
            span.set_attribute("ingestion_id", ingestion_id)
            try:
                pruned = await repo.prune_stale_edges(ingestion_id)
                span.set_attribute("edges_pruned", pruned)
            except Exception as prune_exc:
                logger.warning("Edge pruning failed (non-fatal): %s", prune_exc)
            return {"commit_status": "success", "completion_tracked": True}
        except (Neo4jError, OSError, CircuitOpenError, RuntimeError) as exc:
            span.set_status(StatusCode.ERROR, str(exc))
            span.record_exception(exc)
            logger.error("Neo4j commit failed: %s", exc)
            return {"commit_status": "failed"}
        finally:
            NEO4J_TRANSACTION_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "commit_neo4j"}
            )

async def _process_chunk(
    chunk: List[Dict[str, str]],
) -> Tuple[List[Any], str, Dict[str, str]]:
    initial_state: Dict[str, Any] = {
        "directory_path": "",
        "raw_files": chunk,
        "extracted_nodes": [],
        "extraction_errors": [],
        "validation_retries": 0,
        "commit_status": "",
        "extraction_checkpoint": {},
        "skipped_files": [],
    }
    result = await ingestion_graph.ainvoke(initial_state)
    return (
        result.get("extracted_nodes", []),
        result.get("commit_status", "failed"),
        result.get("extraction_checkpoint", {}),
    )


class StreamingIngestionPipeline:
    def __init__(self, max_bytes: int = 0, chunk_size: int = 50) -> None:
        self._max_bytes = max_bytes or _get_workspace_max_bytes()
        self._chunk_size = chunk_size
        self._chunk_count = 0
        self._total_entities = 0
        self._commit_status = "success"
        self._skipped: List[str] = []

    @property
    def chunk_count(self) -> int:
        return self._chunk_count

    @property
    def total_entities(self) -> int:
        return self._total_entities

    @property
    def commit_status(self) -> str:
        return self._commit_status

    async def process_directory(self, directory_path: str) -> dict:
        for chunk in load_directory_chunked(
            directory_path,
            chunk_size=self._chunk_size,
            max_total_bytes=self._max_bytes,
            skipped=self._skipped,
        ):
            await self._process_single_chunk(chunk)
        return self._build_result()

    async def process_files(self, raw_files: List[Dict[str, str]]) -> dict:
        for i in range(0, len(raw_files), self._chunk_size):
            chunk = raw_files[i:i + self._chunk_size]
            await self._process_single_chunk(chunk)
        return self._build_result()

    async def _process_single_chunk(
        self, chunk: List[Dict[str, str]],
    ) -> None:
        chunk_nodes, chunk_status, _ = await _process_chunk(chunk)
        self._total_entities += len(chunk_nodes)
        if chunk_status == "failed":
            self._commit_status = "failed"
        self._chunk_count += 1

    def _build_result(self) -> dict:
        return {
            "extracted_nodes": [],
            "commit_status": self._commit_status,
            "extraction_errors": [],
            "skipped_files": self._skipped,
        }


async def run_streaming_pipeline(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.streaming_pipeline") as span:
        directory_path = state.get("directory_path", "")
        pipeline = StreamingIngestionPipeline()

        if not directory_path:
            raw_files = state.get("raw_files", [])
            result = await pipeline.process_files(raw_files)
        else:
            result = await pipeline.process_directory(directory_path)

        span.set_attribute("chunk_count", pipeline.chunk_count)
        span.set_attribute("total_entities", pipeline.total_entities)
        if result.get("skipped_files"):
            skipped = result["skipped_files"]
            span.set_attribute("skipped_count", len(skipped))
            logger.warning(
                "Streaming pipeline skipped %d file(s) beyond byte limit: %s",
                len(skipped),
                ", ".join(skipped[:10]),
            )

        return result


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

def _compile_ingestion_graph():
    try:
        checkpointer = get_checkpointer()
    except RuntimeError:
        checkpointer = None
    return builder.compile(checkpointer=checkpointer)


ingestion_graph = _compile_ingestion_graph()
