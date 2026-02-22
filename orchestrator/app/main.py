import base64
import binascii
import logging
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException

from orchestrator.app.graph_builder import ingestion_graph
from orchestrator.app.ingest_models import IngestRequest, IngestResponse
from orchestrator.app.query_engine import query_graph
from orchestrator.app.query_models import QueryRequest, QueryResponse

logger = logging.getLogger(__name__)

app = FastAPI(title="GraphRAG Orchestrator", version="1.0.0")


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


@app.post("/ingest", response_model=IngestResponse)
async def ingest(request: IngestRequest) -> IngestResponse:
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
    return IngestResponse(
        status=result.get("commit_status", "unknown"),
        entities_extracted=len(result.get("extracted_nodes", [])),
        errors=result.get("extraction_errors", []),
    )


@app.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest) -> QueryResponse:
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
    }
    try:
        result = await query_graph.ainvoke(initial_state)
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
