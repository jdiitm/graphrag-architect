import os
import time

import pytest

from orchestrator.app.config import JobStoreConfig
from orchestrator.app.query_models import QueryJobStore
from orchestrator.app.ingest_models import IngestJobStore


class TestJobStoreConfigFromEnv:
    def test_default_ttl_is_3600(self):
        config = JobStoreConfig()
        assert config.ttl_seconds == 3600.0

    def test_from_env_reads_variable(self, monkeypatch):
        monkeypatch.setenv("JOB_STORE_TTL_SECONDS", "7200")
        config = JobStoreConfig.from_env()
        assert config.ttl_seconds == 7200.0

    def test_from_env_defaults_without_variable(self, monkeypatch):
        monkeypatch.delenv("JOB_STORE_TTL_SECONDS", raising=False)
        config = JobStoreConfig.from_env()
        assert config.ttl_seconds == 3600.0


class TestQueryJobStoreHeartbeat:
    @pytest.mark.asyncio
    async def test_heartbeat_extends_lease(self):
        store = QueryJobStore(ttl_seconds=1.0)
        job = await store.create()
        await store.heartbeat(job.job_id)
        mono_ts = store._mono.get(job.job_id)
        assert mono_ts is not None
        assert (time.monotonic() - mono_ts) < 0.5

    @pytest.mark.asyncio
    async def test_heartbeat_nonexistent_job_is_noop(self):
        store = QueryJobStore(ttl_seconds=1.0)
        await store.heartbeat("nonexistent-id")

    @pytest.mark.asyncio
    async def test_running_job_not_evicted_after_heartbeat(self):
        store = QueryJobStore(ttl_seconds=0.2)
        job = await store.create()
        await store.mark_running(job.job_id)
        await store.heartbeat(job.job_id)
        retrieved = await store.get(job.job_id)
        assert retrieved is not None
        assert retrieved.status.value == "running"


class TestIngestJobStoreHeartbeat:
    @pytest.mark.asyncio
    async def test_heartbeat_extends_lease(self):
        store = IngestJobStore(ttl_seconds=1.0)
        job = await store.create()
        await store.heartbeat(job.job_id)
        mono_ts = store._mono.get(job.job_id)
        assert mono_ts is not None
        assert (time.monotonic() - mono_ts) < 0.5

    @pytest.mark.asyncio
    async def test_heartbeat_nonexistent_job_is_noop(self):
        store = IngestJobStore(ttl_seconds=1.0)
        await store.heartbeat("nonexistent-id")


class TestMainUsesConfigurableTTL:
    def test_job_store_ttl_not_hardcoded(self, monkeypatch):
        monkeypatch.setenv("JOB_STORE_TTL_SECONDS", "1800")
        monkeypatch.delenv("REDIS_URL", raising=False)
        config = JobStoreConfig.from_env()
        store = QueryJobStore(ttl_seconds=config.ttl_seconds)
        assert store._ttl == 1800.0
