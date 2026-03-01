import asyncio
import concurrent.futures
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Set, Tuple, TypedDict

from langgraph.graph import END, START, StateGraph
from neo4j.exceptions import Neo4jError
from opentelemetry.trace import StatusCode

from orchestrator.app.ast_dlq import ASTDeadLetterQueue, create_ast_dlq
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
from orchestrator.app.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
)
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
from orchestrator.app.vector_store import create_vector_store, resolve_collection_name
from orchestrator.app.config import VectorStoreConfig
from orchestrator.app.workspace_loader import load_directory_chunked
from orchestrator.app.vector_sync_outbox import (
    CoalescingOutbox,
    DurableOutboxDrainer,
    Neo4jOutboxStore,
    OutboxDrainer,
    RedisOutboxStore,
    VectorSyncEvent,
    VectorSyncOutbox,
)
from orchestrator.app.config import IngestionConfig
from orchestrator.app.distributed_lock import (
    BoundedTaskSet,
    create_ingestion_lock,
)

from orchestrator.app.config import ASTPoolConfig, GRPCASTConfig
from orchestrator.app.ast_grpc_client import GRPCASTClient
from orchestrator.app.ast_result_consumer import ASTResultConsumer

_VECTOR_COLLECTION = "services"

_AST_POOL_CFG = ASTPoolConfig.from_env()
_PROCESS_POOL_MAX_WORKERS = min(
    int(os.environ.get("AST_POOL_WORKERS", "4")),
    _AST_POOL_CFG.ceiling,
)

_USE_REMOTE_AST: bool = os.environ.get("USE_REMOTE_AST", "false").lower() == "true"


class _ASTPoolHolder:
    instance: Optional[concurrent.futures.ProcessPoolExecutor] = None


def _get_process_pool() -> concurrent.futures.ProcessPoolExecutor:
    if _ASTPoolHolder.instance is None:
        _ASTPoolHolder.instance = concurrent.futures.ProcessPoolExecutor(
            max_workers=_PROCESS_POOL_MAX_WORKERS,
        )
    return _ASTPoolHolder.instance


class IngestionDegradedError(RuntimeError):
    def __init__(
        self, message: str, retry_after_seconds: int = 30,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class IngestRejectionError(RuntimeError):
    pass


_DEFAULT_AST_BREAKER_FAILURE_THRESHOLD = 3
_DEFAULT_AST_BREAKER_RECOVERY_TIMEOUT = 30.0

_ast_worker_breaker = CircuitBreaker(
    config=CircuitBreakerConfig(
        failure_threshold=_DEFAULT_AST_BREAKER_FAILURE_THRESHOLD,
        recovery_timeout=_DEFAULT_AST_BREAKER_RECOVERY_TIMEOUT,
        jitter_factor=0.0,
    ),
    name="ast-worker",
)

_AST_DLQ: ASTDeadLetterQueue = create_ast_dlq()


async def enqueue_ast_dlq(payload: Dict[str, Any]) -> None:
    if hasattr(_AST_DLQ, "async_enqueue"):
        await _AST_DLQ.async_enqueue(payload)
    else:
        _AST_DLQ.enqueue(payload)


def get_ast_dlq() -> List[Dict[str, Any]]:
    return _AST_DLQ.peek()


def resolve_vector_collection(tenant_id: Optional[str] = None) -> str:
    return resolve_collection_name(_VECTOR_COLLECTION, tenant_id)

_VECTOR_OUTBOX = VectorSyncOutbox()

_DEFAULT_COALESCING_MAX_ENTRIES = 500


def _spillover_to_vector_outbox(
    events: List[VectorSyncEvent],
) -> None:
    for event in events:
        _VECTOR_OUTBOX.enqueue(event)


def create_durable_spillover_fn(
    store: Any,
) -> Callable[[List[VectorSyncEvent]], None]:
    pending: List[VectorSyncEvent] = []

    def _spillover(events: List[VectorSyncEvent]) -> None:
        pending.extend(events)
        try:
            loop = asyncio.get_running_loop()
            for event in events:
                loop.create_task(store.write_event(event))
        except RuntimeError:
            pass

    _spillover.pending = pending  # type: ignore[attr-defined]
    return _spillover


_COALESCING_OUTBOX = CoalescingOutbox(
    max_entries=_DEFAULT_COALESCING_MAX_ENTRIES,
    spillover_fn=_spillover_to_vector_outbox,
)


def _coalescing_enabled() -> bool:
    return os.environ.get("OUTBOX_COALESCING", "true").lower() == "true"


def _on_background_task_overflow() -> None:
    spilled = _COALESCING_OUTBOX.flush()
    for event in spilled:
        _VECTOR_OUTBOX.enqueue(event)
    if spilled:
        logger.warning(
            "Flushed %d coalescing events to outbox on task overflow",
            len(spilled),
        )


_BACKGROUND_TASKS = BoundedTaskSet(
    max_tasks=50,
    on_overflow=_on_background_task_overflow,
)


def _is_production_mode() -> bool:
    return os.environ.get("DEPLOYMENT_MODE", "").lower() == "production"


def create_outbox_drainer(
    redis_conn: Any,
    vector_store: Any,
    neo4j_driver: Any = None,
) -> OutboxDrainer | DurableOutboxDrainer:
    if neo4j_driver is not None:
        store = Neo4jOutboxStore(driver=neo4j_driver)
        return DurableOutboxDrainer(store=store, vector_store=vector_store)
    if redis_conn is not None:
        store = RedisOutboxStore(redis_conn=redis_conn)
        return DurableOutboxDrainer(store=store, vector_store=vector_store)
    if _is_production_mode():
        raise SystemExit(
            "FATAL: DEPLOYMENT_MODE=production but no durable outbox store "
            "is configured. Set REDIS_URL or provide a Neo4j driver to "
            "prevent silent vector-sync data loss on pod restart."
        )
    return OutboxDrainer(outbox=_VECTOR_OUTBOX, vector_store=vector_store)

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


class _VectorStoreHolder:
    value: Any = None


def get_vector_store() -> Any:
    if _VectorStoreHolder.value is None:
        vs_cfg = VectorStoreConfig.from_env()
        _VectorStoreHolder.value = create_vector_store(
            backend=vs_cfg.backend,
            url=vs_cfg.qdrant_url,
            api_key=vs_cfg.qdrant_api_key,
            pool_size=vs_cfg.pool_size,
            deployment_mode=vs_cfg.deployment_mode,
        )
    return _VectorStoreHolder.value


class _RedisHolder:
    value: Any = None


def _get_redis_conn() -> Any:
    if _RedisHolder.value is not None:
        return _RedisHolder.value
    from orchestrator.app.config import RedisConfig
    redis_cfg = RedisConfig.from_env()
    if not redis_cfg.url:
        return None
    from orchestrator.app.redis_client import create_async_redis
    _RedisHolder.value = create_async_redis(
        redis_cfg.url, password=redis_cfg.password, db=redis_cfg.db,
    )
    return _RedisHolder.value


class _IngestionLockHolder:
    value: Any = None


def get_ingestion_lock() -> Any:
    if _IngestionLockHolder.value is None:
        _IngestionLockHolder.value = create_ingestion_lock()
    return _IngestionLockHolder.value


def set_ingestion_lock(lock: Any) -> None:
    _IngestionLockHolder.value = lock


@asynccontextmanager
async def acquire_ingestion_lock(
    tenant_id: str, namespace: str,
) -> AsyncIterator[None]:
    key = f"{tenant_id}:{namespace}"
    lock = get_ingestion_lock()
    async with lock.acquire(key, ttl=300):
        yield


async def invalidate_caches_after_ingest(
    tenant_id: str = "",
    node_ids: Optional[Set[str]] = None,
) -> None:
    if not tenant_id:
        raise IngestRejectionError(
            "Refusing global cache invalidation: tenant_id is required. "
            "Untagged ingestion payloads must be rejected to prevent "
            "thundering-herd cache collapse."
        )
    try:
        from orchestrator.app.query_engine import (
            _SUBGRAPH_CACHE,
            _SEMANTIC_CACHE,
        )
        if node_ids:
            sg_result = _SUBGRAPH_CACHE.invalidate_by_nodes(node_ids)
        else:
            logger.warning(
                "Tenant-wide cache fallback: no committed node_ids provided "
                "for tenant=%s — invalidating entire tenant cache. "
                "This triggers a thundering-herd risk.",
                tenant_id,
            )
            sg_result = _SUBGRAPH_CACHE.invalidate_tenant(tenant_id)
        if hasattr(sg_result, "__await__"):
            await sg_result
        if _SEMANTIC_CACHE is not None:
            if node_ids and hasattr(_SEMANTIC_CACHE, "invalidate_by_nodes"):
                result = _SEMANTIC_CACHE.invalidate_by_nodes(node_ids)
            else:
                result = _SEMANTIC_CACHE.invalidate_tenant(tenant_id)
            if hasattr(result, "__await__"):
                await result
        logger.info(
            "Cache invalidation complete: tenant=%s, node_ids=%d",
            tenant_id,
            len(node_ids) if node_ids else 0,
        )
    except IngestRejectionError:
        raise
    except Exception as exc:
        logger.warning("Cache invalidation failed (non-fatal): %s", exc)


class IngestionState(TypedDict, total=False):
    directory_path: str
    raw_files: List[Dict[str, str]]
    extracted_nodes: List[Any]
    extraction_errors: List[str]
    validation_retries: int
    commit_status: str
    extraction_checkpoint: Dict[str, str]
    skipped_files: List[str]
    tenant_id: str


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


async def _run_ast_via_remote(
    pending: List[Dict[str, str]],
) -> Tuple[Any, Any]:
    async def _remote_call() -> Tuple[Any, Any]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, _run_ast_extraction, pending,
        )

    return await _ast_worker_breaker.call(_remote_call)


class _GRPCASTClientHolder:
    instance: Optional[GRPCASTClient] = None


def _get_grpc_ast_client() -> GRPCASTClient:
    if _GRPCASTClientHolder.instance is None:
        cfg = GRPCASTConfig.from_env()
        _GRPCASTClientHolder.instance = GRPCASTClient(config=cfg)
    return _GRPCASTClientHolder.instance


def _grpc_ast_endpoint_configured() -> bool:
    return bool(GRPCASTConfig.from_env().endpoint)


def _build_nodes_from_ast_results(
    go_result: Any, py_result: Any, tenant_id: str,
) -> List[Any]:
    nodes: List[Any] = []
    for ast_svc in go_result.services + py_result.services:
        nodes.append(ServiceNode(
            id=ast_svc.service_id,
            name=ast_svc.name,
            language=ast_svc.language,
            framework=ast_svc.framework,
            opentelemetry_enabled=ast_svc.opentelemetry_enabled,
            confidence=1.0,
            tenant_id=tenant_id,
        ))
    for ast_call in go_result.calls + py_result.calls:
        nodes.append(CallsEdge(
            source_service_id=ast_call.source_service_id,
            target_service_id=ast_call.target_hint,
            protocol=ast_call.protocol,
            confidence=1.0,
            tenant_id=tenant_id,
        ))
    return nodes


async def _extract_via_grpc(
    pending: List[Dict[str, str]], tenant_id: str,
) -> List[Any]:
    client = _get_grpc_ast_client()
    file_pairs = [(f["path"], f["content"]) for f in pending]
    ast_results = await client.extract_batch(file_pairs)
    nodes: List[Any] = []
    for result in ast_results:
        extraction = ASTResultConsumer.convert_to_extraction_models(
            result, tenant_id=tenant_id,
        )
        nodes.extend(extraction.services)
        nodes.extend(extraction.calls)
    return nodes


async def parse_source_ast(state: IngestionState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("ingestion.parse_source_ast"):
        start = time.monotonic()
        raw_files = state.get("raw_files", [])
        checkpoint = _load_or_create_checkpoint(state, raw_files)
        pending = checkpoint.filter_files(raw_files, FileStatus.PENDING)
        tenant_id = state.get("tenant_id", "default")

        if _USE_REMOTE_AST and _grpc_ast_endpoint_configured():
            try:
                nodes = await _extract_via_grpc(pending, tenant_id)
            except (CircuitOpenError, ConnectionError, OSError) as exc:
                await enqueue_ast_dlq({
                    "raw_files": pending,
                    "tenant_id": tenant_id,
                })
                grpc_cfg = GRPCASTConfig.from_env()
                raise IngestionDegradedError(
                    f"gRPC AST service unavailable: {exc}",
                    retry_after_seconds=int(grpc_cfg.timeout_seconds),
                ) from exc
        elif _USE_REMOTE_AST:
            try:
                go_result, py_result = await _run_ast_via_remote(pending)
            except (CircuitOpenError, ConnectionError, OSError) as exc:
                await enqueue_ast_dlq({
                    "raw_files": pending,
                    "tenant_id": tenant_id,
                })
                raise IngestionDegradedError(
                    f"Go AST workers unavailable: {exc}",
                    retry_after_seconds=int(
                        _DEFAULT_AST_BREAKER_RECOVERY_TIMEOUT
                    ),
                ) from exc
            nodes = _build_nodes_from_ast_results(
                go_result, py_result, tenant_id,
            )
        else:
            loop = asyncio.get_running_loop()
            go_result, py_result = await loop.run_in_executor(
                _get_process_pool(), _run_ast_extraction, pending,
            )
            nodes = _build_nodes_from_ast_results(
                go_result, py_result, tenant_id,
            )

        extracted_paths = (
            [f["path"] for f in pending if f["path"].endswith(".go")]
            + [f["path"] for f in pending if f["path"].endswith(".py")]
        )
        checkpoint.mark(extracted_paths, FileStatus.EXTRACTED)
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
        tenant_id = state.get("tenant_id", "default")
        manifest_entities = await asyncio.to_thread(
            parse_all_manifests, raw_files, tenant_id,
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


async def _enqueue_vector_cleanup(
    pruned_ids: list, span: Any, tenant_id: str = "",
    neo4j_driver: Any = None,
) -> None:
    if not pruned_ids:
        return
    collection = resolve_vector_collection(tenant_id or None)
    event = VectorSyncEvent(
        collection=collection, pruned_ids=pruned_ids,
    )
    if neo4j_driver is not None:
        store = Neo4jOutboxStore(driver=neo4j_driver)
        await store.write_event(event)
    elif _is_production_mode():
        raise SystemExit(
            "FATAL: DEPLOYMENT_MODE=production but _enqueue_vector_cleanup "
            "called without a durable store (neo4j_driver is None). "
            "In-memory outbox is not safe for production — events would "
            "be lost on pod restart."
        )
    elif _coalescing_enabled():
        _COALESCING_OUTBOX.enqueue(event)
    else:
        _VECTOR_OUTBOX.enqueue(event)
    span.set_attribute("vector_sync_events_queued", 1)
    span.set_attribute("vectors_queued_for_delete", len(pruned_ids))


async def _enqueue_vector_cleanup_durable(
    pruned_ids: list, span: Any, tenant_id: str,
    neo4j_driver: Any,
) -> None:
    collection = resolve_vector_collection(tenant_id or None)
    event = VectorSyncEvent(
        collection=collection, pruned_ids=pruned_ids,
    )
    store = Neo4jOutboxStore(driver=neo4j_driver)
    await store.write_event(event)
    span.set_attribute("vector_sync_events_queued", 1)
    span.set_attribute("vectors_queued_for_delete", len(pruned_ids))


async def drain_vector_outbox() -> int:
    vs = get_vector_store()
    total = 0
    redis_conn = _get_redis_conn()
    try:
        driver = get_driver()
    except RuntimeError:
        driver = None
    durable_drainer = create_outbox_drainer(
        redis_conn=redis_conn, vector_store=vs, neo4j_driver=driver,
    )
    if isinstance(durable_drainer, DurableOutboxDrainer):
        total += await durable_drainer.process_once()
    coalesced = _COALESCING_OUTBOX.drain_pending()
    for event in coalesced:
        _VECTOR_OUTBOX.enqueue(event)
    if _VECTOR_OUTBOX.pending_count > 0:
        inmemory_drainer = OutboxDrainer(
            outbox=_VECTOR_OUTBOX, vector_store=vs,
        )
        total += await inmemory_drainer.process_once()
    if not isinstance(durable_drainer, DurableOutboxDrainer) and total == 0:
        total += await durable_drainer.process_once()
    return total


def _get_sink_batch_size() -> int:
    return IngestionConfig.from_env().sink_batch_size


async def _post_commit_side_effects(
    repo: GraphRepository,
    ingestion_id: str,
    tenant_id: str,
    span: Any,
    committed_node_ids: Optional[Set[str]] = None,
    neo4j_driver: Any = None,
) -> None:
    try:
        pruned_count, pruned_ids = await repo.prune_stale_edges(ingestion_id)
        span.set_attribute("edges_pruned", pruned_count)
        if pruned_ids and neo4j_driver is not None:
            await _enqueue_vector_cleanup_durable(
                pruned_ids, span, tenant_id, neo4j_driver,
            )
        else:
            await _enqueue_vector_cleanup(
                pruned_ids, span, tenant_id=tenant_id,
                neo4j_driver=None,
            )
        task = asyncio.create_task(_safe_drain_vector_outbox())
        if not _BACKGROUND_TASKS.try_add(task):
            logger.warning("Background task limit reached; vector drain skipped")
    except Exception as prune_exc:
        logger.warning("Edge pruning failed (non-fatal): %s", prune_exc)
    try:
        await invalidate_caches_after_ingest(
            tenant_id=tenant_id, node_ids=committed_node_ids,
        )
    except IngestRejectionError as rej_exc:
        logger.warning(
            "Cache invalidation rejected (non-fatal): %s", rej_exc,
        )


async def _safe_drain_vector_outbox() -> None:
    try:
        await drain_vector_outbox()
    except Exception as exc:
        logger.warning("Background vector drain failed (non-fatal): %s", exc)


_DEFAULT_PERIODIC_DRAIN_INTERVAL = 10.0


class PeriodicVectorDrainer:
    def __init__(
        self,
        drain_fn: Any = None,
        interval_seconds: float = _DEFAULT_PERIODIC_DRAIN_INTERVAL,
    ) -> None:
        self._drain_fn = drain_fn or _safe_drain_vector_outbox
        self._interval = interval_seconds
        self._task: Optional[asyncio.Task[None]] = None
        self._stopped = False
        self._wake: Optional[asyncio.Event] = None

    def notify(self) -> None:
        if self._wake is not None:
            self._wake.set()

    def start(self) -> asyncio.Task[None]:
        self._stopped = False
        self._wake = asyncio.Event()
        self._task = asyncio.create_task(self._loop())
        return self._task

    def stop(self) -> None:
        self._stopped = True
        if self._wake is not None:
            self._wake.set()
        if self._task is not None and not self._task.done():
            self._task.cancel()

    async def _loop(self) -> None:
        while not self._stopped:
            try:
                await self._drain_fn()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning(
                    "Periodic vector drain error (non-fatal): %s", exc,
                )
            try:
                await asyncio.wait_for(
                    self._wake.wait(),  # type: ignore[union-attr]
                    timeout=self._interval,
                )
                self._wake.clear()  # type: ignore[union-attr]
            except asyncio.TimeoutError:
                pass


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
            sink = IncrementalNodeSink(
                repo, batch_size=_get_sink_batch_size(),
            )
            await sink.ingest(entities)
            await sink.flush()
            span.set_attribute("entity_count", sink.total_entities)
            span.set_attribute("flush_count", sink.flush_count)
            span.set_attribute("ingestion_id", ingestion_id)
            tenant_id = state.get("tenant_id", "")
            committed_node_ids = {
                e.id for e in entities if hasattr(e, "id")
            }
            await _post_commit_side_effects(
                repo, ingestion_id, tenant_id, span,
                committed_node_ids=committed_node_ids or None,
                neo4j_driver=driver,
            )
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
