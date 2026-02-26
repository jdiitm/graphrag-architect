import time
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from orchestrator.app.query_models import JobStatus


class SourceType(str, Enum):
    SOURCE_CODE = "source_code"
    K8S_MANIFEST = "k8s_manifest"
    KAFKA_SCHEMA = "kafka_schema"


class IngestDocument(BaseModel):
    file_path: str
    content: str
    source_type: SourceType
    repository: Optional[str] = None
    commit_sha: Optional[str] = None


class IngestRequest(BaseModel):
    documents: List[IngestDocument] = Field(..., min_length=1)


class IngestResponse(BaseModel):
    status: str
    entities_extracted: int
    errors: List[str]


class IngestJobResponse(BaseModel):
    job_id: str
    status: JobStatus
    result: Optional[IngestResponse] = None
    error: Optional[str] = None
    created_at: float
    completed_at: Optional[float] = None


class IngestJobStore:
    def __init__(self, ttl_seconds: float = 300.0) -> None:
        self._jobs: Dict[str, IngestJobResponse] = {}
        self._mono: Dict[str, float] = {}
        self._ttl = ttl_seconds

    async def create(self) -> IngestJobResponse:
        self._evict_expired()
        job = IngestJobResponse(
            job_id=str(uuid.uuid4()),
            status=JobStatus.PENDING,
            created_at=time.time(),
        )
        self._jobs[job.job_id] = job
        self._mono[job.job_id] = time.monotonic()
        return job

    async def get(self, job_id: str) -> Optional[IngestJobResponse]:
        return self._jobs.get(job_id)

    async def mark_running(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if job:
            job.status = JobStatus.RUNNING

    async def complete(self, job_id: str, result: IngestResponse) -> None:
        job = self._jobs.get(job_id)
        if job:
            job.status = JobStatus.COMPLETED
            job.result = result
            job.completed_at = time.time()

    async def fail(self, job_id: str, error: str) -> None:
        job = self._jobs.get(job_id)
        if job:
            job.status = JobStatus.FAILED
            job.error = error
            job.completed_at = time.time()

    def _evict_expired(self) -> None:
        now = time.monotonic()
        expired = [
            jid for jid, mono_ts in self._mono.items()
            if (now - mono_ts) > self._ttl
        ]
        for jid in expired:
            self._jobs.pop(jid, None)
            self._mono.pop(jid, None)


def create_ingest_job_store(ttl_seconds: float = 300.0) -> Any:
    return IngestJobStore(ttl_seconds=ttl_seconds)
