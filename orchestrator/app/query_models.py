import time
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from typing_extensions import TypedDict


class QueryComplexity(str, Enum):
    ENTITY_LOOKUP = "entity_lookup"
    SINGLE_HOP = "single_hop"
    MULTI_HOP = "multi_hop"
    AGGREGATE = "aggregate"


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1)
    max_results: int = Field(default=10, ge=1, le=100)


class QueryResponse(BaseModel):
    answer: str
    sources: List[Dict[str, Any]]
    complexity: QueryComplexity
    retrieval_path: str
    evaluation_score: float = -1.0
    retrieval_quality: str = "skipped"


class QueryJobResponse(BaseModel):
    job_id: str
    status: JobStatus
    result: Optional[QueryResponse] = None
    error: Optional[str] = None
    created_at: float
    completed_at: Optional[float] = None


class QueryState(TypedDict):
    query: str
    max_results: int
    complexity: QueryComplexity
    retrieval_path: str
    candidates: List[Dict[str, Any]]
    cypher_query: str
    cypher_results: List[Dict[str, Any]]
    iteration_count: int
    answer: str
    sources: List[Dict[str, Any]]
    authorization: str
    evaluation_score: float
    retrieval_quality: str


class QueryJobStore:
    def __init__(self, ttl_seconds: float = 300.0) -> None:
        self._jobs: Dict[str, QueryJobResponse] = {}
        self._ttl = ttl_seconds

    def create(self) -> QueryJobResponse:
        self._evict_expired()
        job = QueryJobResponse(
            job_id=str(uuid.uuid4()),
            status=JobStatus.PENDING,
            created_at=time.monotonic(),
        )
        self._jobs[job.job_id] = job
        return job

    def get(self, job_id: str) -> Optional[QueryJobResponse]:
        return self._jobs.get(job_id)

    def mark_running(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if job:
            job.status = JobStatus.RUNNING

    def complete(self, job_id: str, result: QueryResponse) -> None:
        job = self._jobs.get(job_id)
        if job:
            job.status = JobStatus.COMPLETED
            job.result = result
            job.completed_at = time.monotonic()

    def fail(self, job_id: str, error: str) -> None:
        job = self._jobs.get(job_id)
        if job:
            job.status = JobStatus.FAILED
            job.error = error
            job.completed_at = time.monotonic()

    def _evict_expired(self) -> None:
        now = time.monotonic()
        expired = [
            jid for jid, job in self._jobs.items()
            if (now - job.created_at) > self._ttl
        ]
        for jid in expired:
            del self._jobs[jid]
