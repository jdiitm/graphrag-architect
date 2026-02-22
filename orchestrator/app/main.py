import base64
import binascii
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException

from orchestrator.app.graph_builder import ingestion_graph
from orchestrator.app.ingest_models import IngestRequest, IngestResponse

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
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return IngestResponse(
        status=result.get("commit_status", "unknown"),
        entities_extracted=len(result.get("extracted_nodes", [])),
        errors=result.get("extraction_errors", []),
    )
