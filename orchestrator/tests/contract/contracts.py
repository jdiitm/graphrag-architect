from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Type

from pydantic import BaseModel


class GoIngestDocument(BaseModel):
    """Mirrors Go ``ingestDocument`` struct json tags exactly.

    See workers/ingestion/internal/processor/forwarding.go.
    """
    file_path: str
    content: str
    source_type: str
    repository: Optional[str] = None
    commit_sha: Optional[str] = None


class GoIngestRequest(BaseModel):
    """Mirrors Go ``ingestRequest`` struct json tags."""
    documents: List[GoIngestDocument]


class ExpectedIngestResponse(BaseModel):
    """Schema Go workers parse from a 200 sync-ingest response."""
    status: str
    entities_extracted: int
    errors: List[str]


class ExpectedAsyncResponse(BaseModel):
    """Schema returned by 202 async-ingest acceptance."""
    job_id: str
    status: str


class ExpectedHealthResponse(BaseModel):
    """Schema Go healthcheck expects from GET /health."""
    status: str


class ErrorResponse(BaseModel):
    """Standard FastAPI error envelope (4xx / 5xx)."""
    detail: str


class RetryBehavior(str, Enum):
    RETRY = "retry"
    DLQ = "dlq"


STATUS_CODE_RETRY_MAP: Dict[int, RetryBehavior] = {
    400: RetryBehavior.DLQ,
    413: RetryBehavior.DLQ,
    422: RetryBehavior.DLQ,
    429: RetryBehavior.RETRY,
    500: RetryBehavior.DLQ,
    503: RetryBehavior.RETRY,
}


@dataclass(frozen=True)
class EndpointContract:
    method: str
    path: str
    success_status: int
    content_type: str = "application/json"
    request_model: Optional[Type[BaseModel]] = None
    success_response_model: Optional[Type[BaseModel]] = None


ENDPOINT_CONTRACTS: Dict[str, EndpointContract] = {
    "ingest_sync": EndpointContract(
        method="POST",
        path="/ingest",
        success_status=200,
        request_model=GoIngestRequest,
        success_response_model=ExpectedIngestResponse,
    ),
    "ingest_async": EndpointContract(
        method="POST",
        path="/ingest",
        success_status=202,
        request_model=GoIngestRequest,
        success_response_model=ExpectedAsyncResponse,
    ),
    "health": EndpointContract(
        method="GET",
        path="/health",
        success_status=200,
        success_response_model=ExpectedHealthResponse,
    ),
}
