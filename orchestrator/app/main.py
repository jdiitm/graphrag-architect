import asyncio
import base64
import binascii
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, Generator, List, Optional

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from orchestrator.app.access_control import (
    AuthConfigurationError,
    InvalidTokenError,
    SecurityPrincipal,
)
from orchestrator.app.circuit_breaker import CircuitOpenError
from orchestrator.app.tenant_isolation import TenantContext
from orchestrator.app.config import AuthConfig, KafkaConsumerConfig, RateLimitConfig
from orchestrator.app.executor import shutdown_pool
from orchestrator.app.graph_builder import ingestion_graph, run_streaming_pipeline
from orchestrator.app.ingest_models import IngestRequest, IngestResponse
from orchestrator.app.kafka_consumer import AsyncKafkaConsumer
from orchestrator.app.checkpoint_store import close_checkpointer, init_checkpointer
from orchestrator.app.neo4j_pool import close_driver, init_driver
from orchestrator.app.observability import configure_metrics, configure_telemetry
from orchestrator.app.query_engine import get_eval_store, query_graph
from orchestrator.app.query_models import (
    QueryJobResponse,
    QueryRequest,
    QueryResponse,
    create_job_store,
)
from orchestrator.app.token_bucket import create_rate_limiter

logger = logging.getLogger(__name__)

_STATE: Dict[str, Any] = {"semaphore": None, "kafka_consumer": None, "kafka_task": None}
_JOB_STORE = create_job_store(ttl_seconds=300.0)
_TENANT_LIMITER = create_rate_limiter(
    capacity=int(os.environ.get("RATE_LIMIT_CAPACITY", "20")),
    refill_rate=float(os.environ.get("RATE_LIMIT_REFILL_RATE", "10.0")),
)


def get_ingestion_semaphore() -> asyncio.Semaphore:
    if _STATE["semaphore"] is None:
        cfg = RateLimitConfig.from_env()
        _STATE["semaphore"] = asyncio.Semaphore(cfg.max_concurrent_ingestions)
    return _STATE["semaphore"]


def set_ingestion_semaphore(sem: Optional[asyncio.Semaphore]) -> None:
    _STATE["semaphore"] = sem


def _kafka_consumer_enabled() -> bool:
    return os.environ.get("KAFKA_CONSUMER_ENABLED", "false").lower() == "true"


async def _kafka_ingest_callback(raw_files: List[Dict[str, str]]) -> Dict[str, Any]:
    initial_state: Dict[str, Any] = {
        "directory_path": "",
        "raw_files": raw_files,
        "extracted_nodes": [],
        "extraction_errors": [],
        "validation_retries": 0,
        "commit_status": "",
        "tenant_id": "default",
    }
    result = await ingestion_graph.ainvoke(initial_state)
    return {
        "commit_status": result.get("commit_status", "unknown"),
        "entities_extracted": len(result.get("extracted_nodes", [])),
    }


@asynccontextmanager
async def lifespan(_app: FastAPI):
    auth = _validate_startup_security()
    configure_telemetry()
    configure_metrics()
    init_checkpointer()
    init_driver()
    set_ingestion_semaphore(
        asyncio.Semaphore(RateLimitConfig.from_env().max_concurrent_ingestions)
    )
    _warn_insecure_auth(auth)
    if _kafka_consumer_enabled():
        kafka_config = KafkaConsumerConfig.from_env()
        consumer = AsyncKafkaConsumer(kafka_config, _kafka_ingest_callback)
        _STATE["kafka_consumer"] = consumer
        _STATE["kafka_task"] = asyncio.create_task(consumer.start())
        logger.info(
            "Kafka consumer started: brokers=%s topic=%s",
            kafka_config.brokers, kafka_config.topic,
        )
    try:
        yield
    finally:
        if _STATE.get("kafka_consumer") is not None:
            await _STATE["kafka_consumer"].stop()
            if _STATE.get("kafka_task") is not None:
                _STATE["kafka_task"].cancel()
            _STATE["kafka_consumer"] = None
            _STATE["kafka_task"] = None
        shutdown_pool()
        await close_driver()
        close_checkpointer()


def _validate_startup_security() -> AuthConfig:
    auth = AuthConfig.from_env()
    if auth.require_tokens and not auth.token_secret:
        raise SystemExit(
            "FATAL: Auth is fail-closed (require_tokens=true) but "
            "AUTH_TOKEN_SECRET is not set. Set AUTH_TOKEN_SECRET or "
            "explicitly set AUTH_REQUIRE_TOKENS=false for development."
        )
    return auth


def _warn_insecure_auth(auth: AuthConfig) -> None:
    if not auth.token_secret:
        logger.warning(
            "AUTH_TOKEN_SECRET is not set. Token verification is disabled. "
            "Set AUTH_TOKEN_SECRET for production deployments."
        )


app = FastAPI(title="GraphRAG Orchestrator", version="1.0.0", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app)


MAX_INGEST_PAYLOAD_BYTES = int(
    os.environ.get("MAX_INGEST_PAYLOAD_BYTES", str(100 * 1024 * 1024))
)


def _iter_decoded_documents(
    request: IngestRequest,
) -> Generator[Dict[str, str], None, None]:
    cumulative = 0
    for doc in request.documents:
        try:
            decoded_bytes = base64.b64decode(doc.content, validate=True)
            decoded_content = decoded_bytes.decode("utf-8")
        except (binascii.Error, UnicodeDecodeError) as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid base64 content for file '{doc.file_path}': {exc}",
            ) from exc
        cumulative += len(decoded_bytes)
        if cumulative > MAX_INGEST_PAYLOAD_BYTES:
            raise HTTPException(
                status_code=413,
                detail=(
                    f"Payload exceeds maximum ingest size of "
                    f"{MAX_INGEST_PAYLOAD_BYTES} bytes"
                ),
            )
        yield {"path": doc.file_path, "content": decoded_content}


def _decode_documents(request: IngestRequest) -> List[Dict[str, str]]:
    return list(_iter_decoded_documents(request))


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "healthy"}


@app.get("/metrics")
def prometheus_metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


def _verify_ingest_auth(authorization: Optional[str]) -> None:
    auth = AuthConfig.from_env()
    if not auth.token_secret:
        if auth.require_tokens:
            raise HTTPException(
                status_code=503,
                detail="server misconfigured: token verification required but no secret set",
            )
        return
    if not authorization or not authorization.strip():
        raise HTTPException(status_code=401, detail="missing authorization token")
    try:
        SecurityPrincipal.from_header(authorization, token_secret=auth.token_secret)
    except InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


@app.post("/ingest", response_model=IngestResponse)
async def ingest(
    request: IngestRequest,
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    _verify_ingest_auth(authorization)
    sem = get_ingestion_semaphore()
    if sem.locked():
        raise HTTPException(
            status_code=429,
            detail="Too many concurrent ingestion requests",
        )
    await sem.acquire()
    try:
        result = await _run_ingestion(request)
        if isinstance(result, JSONResponse):
            result.headers["Deprecation"] = "true"
            return result
        status_code = 200
        body = result.model_dump()
        response = JSONResponse(content=body, status_code=status_code)
        response.headers["Deprecation"] = "true"
        return response
    except CircuitOpenError:
        return JSONResponse(
            status_code=503,
            content={"detail": "service temporarily unavailable"},
            headers={"Retry-After": "30"},
        )
    finally:
        sem.release()


async def _run_streaming_ingestion(directory_path: str) -> IngestResponse:
    try:
        result = await run_streaming_pipeline({
            "directory_path": directory_path,
        })
    except Exception as exc:
        logger.exception("Streaming ingestion failed")
        raise HTTPException(
            status_code=500, detail="Internal ingestion error"
        ) from exc
    return IngestResponse(
        status=result.get("commit_status", "unknown"),
        entities_extracted=len(result.get("extracted_nodes", [])),
        errors=result.get("extraction_errors", []),
    )


async def _run_ingestion(request: IngestRequest) -> IngestResponse:
    raw_files = _decode_documents(request)
    initial_state: Dict[str, Any] = {
        "directory_path": "",
        "raw_files": raw_files,
        "extracted_nodes": [],
        "extraction_errors": [],
        "validation_retries": 0,
        "commit_status": "",
        "tenant_id": "default",
    }
    try:
        result = await ingestion_graph.ainvoke(initial_state)
    except Exception as exc:
        logger.exception("Ingestion graph failed")
        raise HTTPException(
            status_code=500, detail="Internal ingestion error"
        ) from exc
    response = IngestResponse(
        status=result.get("commit_status", "unknown"),
        entities_extracted=len(result.get("extracted_nodes", [])),
        errors=result.get("extraction_errors", []),
    )
    if result.get("commit_status") == "failed":
        return JSONResponse(
            status_code=503, content=response.model_dump()
        )
    return response


def _resolve_tenant_context(authorization: Optional[str]) -> TenantContext:
    auth = AuthConfig.from_env()
    if not authorization:
        if auth.require_tokens:
            raise HTTPException(
                status_code=401,
                detail="authorization header required",
            )
        return TenantContext.default()
    if not auth.token_secret:
        if auth.require_tokens:
            raise HTTPException(
                status_code=503,
                detail="server misconfigured: token verification required but no secret set",
            )
        return TenantContext.default()
    try:
        principal = SecurityPrincipal.from_header(
            authorization, token_secret=auth.token_secret,
        )
        return TenantContext.default(tenant_id=principal.team)
    except InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except AuthConfigurationError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


def _build_query_state(
    request: QueryRequest, authorization: str,
) -> Dict[str, Any]:
    tenant_ctx = _resolve_tenant_context(authorization)
    return {
        "query": request.query,
        "max_results": request.max_results,
        "complexity": "",
        "retrieval_path": "",
        "candidates": [],
        "cypher_query": "",
        "cypher_results": [],
        "iteration_count": 0,
        "answer": "",
        "sources": [],
        "authorization": authorization,
        "evaluation_score": None,
        "retrieval_quality": "skipped",
        "query_id": "",
        "tenant_id": tenant_ctx.tenant_id,
    }


def _result_to_response(result: Dict[str, Any]) -> QueryResponse:
    return QueryResponse(
        answer=result.get("answer", ""),
        sources=result.get("sources", []),
        complexity=result.get("complexity", "entity_lookup"),
        retrieval_path=result.get("retrieval_path", "vector"),
        evaluation_score=result.get("evaluation_score"),
        retrieval_quality=result.get("retrieval_quality", "skipped"),
        query_id=result.get("query_id", ""),
    )


async def _enforce_rate_limit(tenant_id: str) -> None:
    if not await _TENANT_LIMITER.try_acquire(tenant_id):
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded for tenant",
        )


@app.post("/query", response_model=QueryResponse)
async def query(
    request: QueryRequest,
    authorization: Optional[str] = Header(default=None),
    async_mode: bool = False,
) -> JSONResponse:
    tenant_ctx = _resolve_tenant_context(authorization)
    await _enforce_rate_limit(tenant_ctx.tenant_id)
    if async_mode:
        job = await _JOB_STORE.create()
        initial_state = _build_query_state(request, authorization or "")
        asyncio.create_task(
            _run_query_job(job.job_id, initial_state)
        )
        return JSONResponse(
            status_code=202,
            content={"job_id": job.job_id, "status": job.status.value},
        )

    initial_state = _build_query_state(request, authorization or "")
    try:
        result = await query_graph.ainvoke(initial_state)
    except AuthConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Query graph failed")
        raise HTTPException(
            status_code=500, detail="Internal query error"
        ) from exc
    response = _result_to_response(result)
    return JSONResponse(content=response.model_dump(), status_code=200)


async def _run_query_job(job_id: str, initial_state: Dict[str, Any]) -> None:
    await _JOB_STORE.mark_running(job_id)
    try:
        result = await query_graph.ainvoke(initial_state)
        response = _result_to_response(result)
        await _JOB_STORE.complete(job_id, response)
    except Exception as exc:
        logger.exception("Background query job %s failed", job_id)
        await _JOB_STORE.fail(job_id, str(exc))


@app.get("/query/{job_id}", response_model=QueryJobResponse)
async def get_query_job(job_id: str) -> JSONResponse:
    job = await _JOB_STORE.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse(content=job.model_dump(), status_code=200)


_EVAL_STORE = get_eval_store()


@app.get("/query/{query_id}/evaluation")
async def get_evaluation(query_id: str) -> JSONResponse:
    result = _EVAL_STORE.get(query_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Evaluation not ready or not found")
    return JSONResponse(content=result, status_code=200)
