import logging
import time
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from typing_extensions import TypedDict

from orchestrator.app.redis_client import create_async_redis, require_redis

logger = logging.getLogger(__name__)


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
    evaluation_score: Optional[float] = None
    retrieval_quality: str = "skipped"
    query_id: str = ""


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
    evaluation_score: Optional[float]
    retrieval_quality: str
    query_id: str


class QueryJobStore:
    def __init__(self, ttl_seconds: float = 300.0) -> None:
        self._jobs: Dict[str, QueryJobResponse] = {}
        self._mono: Dict[str, float] = {}
        self._ttl = ttl_seconds

    async def create(self) -> QueryJobResponse:
        self._evict_expired()
        job = QueryJobResponse(
            job_id=str(uuid.uuid4()),
            status=JobStatus.PENDING,
            created_at=time.time(),
        )
        self._jobs[job.job_id] = job
        self._mono[job.job_id] = time.monotonic()
        return job

    async def get(self, job_id: str) -> Optional[QueryJobResponse]:
        return self._jobs.get(job_id)

    async def mark_running(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if job:
            job.status = JobStatus.RUNNING

    async def complete(self, job_id: str, result: QueryResponse) -> None:
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


class RedisQueryJobStore:
    def __init__(
        self,
        redis_url: str,
        ttl_seconds: int = 300,
        key_prefix: str = "graphrag:jobstore:",
        password: str = "",
        db: int = 0,
    ) -> None:
        require_redis("RedisQueryJobStore")
        self._redis = create_async_redis(redis_url, password=password, db=db)
        self._ttl = ttl_seconds
        self._prefix = key_prefix

    def _rkey(self, job_id: str) -> str:
        return f"{self._prefix}{job_id}"

    async def create(self) -> QueryJobResponse:
        job = QueryJobResponse(
            job_id=str(uuid.uuid4()),
            status=JobStatus.PENDING,
            created_at=time.time(),
        )
        try:
            await self._redis.setex(
                self._rkey(job.job_id), self._ttl, job.model_dump_json(),
            )
        except Exception:
            logger.debug("Redis job-store create failed")
        return job

    async def get(self, job_id: str) -> Optional[QueryJobResponse]:
        try:
            raw = await self._redis.get(self._rkey(job_id))
            if raw is not None:
                return QueryJobResponse.model_validate_json(raw)
        except Exception:
            logger.debug("Redis job-store get failed")
        return None

    async def mark_running(self, job_id: str) -> None:
        job = await self.get(job_id)
        if job is None:
            return
        job.status = JobStatus.RUNNING
        try:
            await self._redis.setex(
                self._rkey(job_id), self._ttl, job.model_dump_json(),
            )
        except Exception:
            logger.debug("Redis job-store mark_running failed")

    async def complete(self, job_id: str, result: QueryResponse) -> None:
        job = await self.get(job_id)
        if job is None:
            return
        job.status = JobStatus.COMPLETED
        job.result = result
        job.completed_at = time.time()
        try:
            await self._redis.setex(
                self._rkey(job_id), self._ttl, job.model_dump_json(),
            )
        except Exception:
            logger.debug("Redis job-store complete failed")

    async def fail(self, job_id: str, error: str) -> None:
        job = await self.get(job_id)
        if job is None:
            return
        job.status = JobStatus.FAILED
        job.error = error
        job.completed_at = time.time()
        try:
            await self._redis.setex(
                self._rkey(job_id), self._ttl, job.model_dump_json(),
            )
        except Exception:
            logger.debug("Redis job-store fail failed")


def create_job_store(ttl_seconds: float = 300.0) -> Any:
    from orchestrator.app.config import RedisConfig
    redis_cfg = RedisConfig.from_env()
    if redis_cfg.url:
        return RedisQueryJobStore(
            redis_url=redis_cfg.url,
            password=redis_cfg.password,
            db=redis_cfg.db,
            ttl_seconds=int(ttl_seconds),
        )
    return QueryJobStore(ttl_seconds=ttl_seconds)
