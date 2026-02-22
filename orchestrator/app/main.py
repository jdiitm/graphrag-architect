import base64
import binascii
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from orchestrator.app.access_control import (
    InvalidTokenError,
    SecurityPrincipal,
)
from orchestrator.app.graph_builder import ingestion_graph
from orchestrator.app.ingest_models import IngestRequest, IngestResponse
from orchestrator.app.neo4j_pool import close_driver, init_driver
from orchestrator.app.observability import configure_metrics, configure_telemetry
from orchestrator.app.query_engine import query_graph
from orchestrator.app.query_models import QueryRequest, QueryResponse

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    configure_telemetry()
    configure_metrics()
    init_driver()
    try:
        yield
    finally:
        await close_driver()


app = FastAPI(title="GraphRAG Orchestrator", version="1.0.0", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app)


def _decode_documents(request: IngestRequest) -> List[Dict[str, str]]:
    raw_files: List[Dict[str, str]] = []
    for doc in request.documents:
        try:
            decoded_bytes = base64.b64decode(doc.content, validate=True)
            decoded_content = decoded_bytes.decode("utf-8")
        except (binascii.Error, UnicodeDecodeError) as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid base64 content for file '{doc.file_path}': {exc}",
            ) from exc
        raw_files.append({"path": doc.file_path, "content": decoded_content})
    return raw_files


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "healthy"}


@app.get("/metrics")
def prometheus_metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


def _verify_ingest_auth(authorization: Optional[str]) -> None:
    secret = os.environ.get("AUTH_TOKEN_SECRET", "")
    if not secret:
        return
    if not authorization or not authorization.strip():
        raise HTTPException(status_code=401, detail="missing authorization token")
    try:
        SecurityPrincipal.from_header(authorization, token_secret=secret)
    except InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


@app.post("/ingest", response_model=IngestResponse)
async def ingest(
    request: IngestRequest,
    authorization: Optional[str] = Header(default=None),
) -> IngestResponse:
    _verify_ingest_auth(authorization)
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
    }
    try:
        result = await query_graph.ainvoke(initial_state)
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
    )
