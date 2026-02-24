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
from orchestrator.app.config import AuthConfig, RateLimitConfig
from orchestrator.app.executor import shutdown_pool
from orchestrator.app.graph_builder import ingestion_graph
from orchestrator.app.ingest_models import IngestRequest, IngestResponse
from orchestrator.app.neo4j_pool import close_driver, init_driver
from orchestrator.app.observability import configure_metrics, configure_telemetry
from orchestrator.app.query_engine import query_graph
from orchestrator.app.query_models import QueryRequest, QueryResponse

logger = logging.getLogger(__name__)

_STATE: Dict[str, Any] = {"semaphore": None}


def get_ingestion_semaphore() -> asyncio.Semaphore:
    if _STATE["semaphore"] is None:
        cfg = RateLimitConfig.from_env()
        _STATE["semaphore"] = asyncio.Semaphore(cfg.max_concurrent_ingestions)
    return _STATE["semaphore"]


def set_ingestion_semaphore(sem: Optional[asyncio.Semaphore]) -> None:
    _STATE["semaphore"] = sem


@asynccontextmanager
async def lifespan(_app: FastAPI):
    auth = _validate_startup_security()
    configure_telemetry()
    configure_metrics()
    init_driver()
    set_ingestion_semaphore(
        asyncio.Semaphore(RateLimitConfig.from_env().max_concurrent_ingestions)
    )
    _warn_insecure_auth(auth)
    try:
        yield
    finally:
        shutdown_pool()
        await close_driver()


def _validate_startup_security() -> AuthConfig:
    return AuthConfig.from_env()


def _warn_insecure_auth(auth: AuthConfig) -> None:
    if not auth.token_secret:
        if auth.require_tokens:
            logger.error(
                "AUTH_REQUIRE_TOKENS is true but AUTH_TOKEN_SECRET is not set. "
                "All authenticated endpoints will reject requests."
            )
        else:
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
) -> IngestResponse:
    _verify_ingest_auth(authorization)
    sem = get_ingestion_semaphore()
    if sem.locked():
        raise HTTPException(
            status_code=429,
            detail="Too many concurrent ingestion requests",
        )
    await sem.acquire()
    try:
        return await _run_ingestion(request)
    finally:
        sem.release()


async def _run_ingestion(request: IngestRequest) -> IngestResponse:
    raw_files = _decode_documents(request)
    initial_state: Dict[str, Any] = {
        "directory_path": "",
        "raw_files": raw_files,
        "extracted_nodes": [],
        "extraction_errors": [],
        "validation_retries": 0,
        "commit_status": "",
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


@app.post("/query", response_model=QueryResponse)
async def query(
    request: QueryRequest,
    authorization: Optional[str] = Header(default=None),
) -> QueryResponse:
    initial_state: Dict[str, Any] = {
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
        "authorization": authorization or "",
        "evaluation_score": -1.0,
        "retrieval_quality": "skipped",
    }
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
    return QueryResponse(
        answer=result.get("answer", ""),
        sources=result.get("sources", []),
        complexity=result.get("complexity", "entity_lookup"),
        retrieval_path=result.get("retrieval_path", "vector"),
        evaluation_score=result.get("evaluation_score", -1.0),
        retrieval_quality=result.get("retrieval_quality", "skipped"),
    )
