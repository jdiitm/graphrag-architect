import asyncio
import os
import time

import pytest

from orchestrator.app.config import JobStoreConfig
from orchestrator.app.ingest_models import IngestJobStore
from orchestrator.app.query_models import QueryJobStore


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

    @pytest.mark.asyncio
    async def test_get_evicts_expired_jobs_without_new_create(self):
        store = QueryJobStore(ttl_seconds=0.01)
        job = await store.create()
        time.sleep(0.02)
        retrieved = await store.get(job.job_id)
        assert retrieved is None


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

    @pytest.mark.asyncio
    async def test_get_evicts_expired_jobs_without_new_create(self):
        store = IngestJobStore(ttl_seconds=0.01)
        job = await store.create()
        time.sleep(0.02)
        retrieved = await store.get(job.job_id)
        assert retrieved is None


class TestMainUsesConfigurableTTL:
    def test_job_store_ttl_not_hardcoded(self, monkeypatch):
        monkeypatch.setenv("JOB_STORE_TTL_SECONDS", "1800")
        monkeypatch.delenv("REDIS_URL", raising=False)
        config = JobStoreConfig.from_env()
        store = QueryJobStore(ttl_seconds=config.ttl_seconds)
        assert store._ttl == 1800.0


class TestHeartbeatWiredInProduction:
    @pytest.mark.asyncio
    async def test_run_query_job_calls_heartbeat(self):
        from unittest.mock import AsyncMock, MagicMock, patch
        store = QueryJobStore(ttl_seconds=3600.0)
        job = await store.create()
        mock_graph = AsyncMock(return_value={
            "answer": "test", "sources": [],
            "complexity": "entity_lookup", "retrieval_path": "vector",
        })
        with patch("orchestrator.app.main._JOB_STORE", store), \
             patch("orchestrator.app.main.query_graph") as mg:
            mg.ainvoke = mock_graph
            store.heartbeat = AsyncMock()
            from orchestrator.app.main import _run_query_job
            await _run_query_job(job.job_id, {"query": "test"})
            store.heartbeat.assert_called_with(job.job_id)

    @pytest.mark.asyncio
    async def test_run_ingest_job_calls_heartbeat(self):
        from unittest.mock import AsyncMock, MagicMock, patch
        store = IngestJobStore(ttl_seconds=3600.0)
        job = await store.create()
        mock_graph = AsyncMock(return_value={
            "commit_status": "success", "extracted_nodes": [],
            "extraction_errors": [],
        })
        mock_sem = AsyncMock()
        mock_sem.try_acquire = AsyncMock(return_value=(True, "tok"))
        mock_sem.release = AsyncMock()
        with patch("orchestrator.app.main._INGEST_JOB_STORE", store), \
             patch("orchestrator.app.main.ingestion_graph") as mg, \
             patch("orchestrator.app.main.get_ingestion_semaphore", return_value=mock_sem):
            mg.ainvoke = mock_graph
            store.heartbeat = AsyncMock()
            from orchestrator.app.main import _run_ingest_job
            await _run_ingest_job(job.job_id, [])
            store.heartbeat.assert_called_with(job.job_id)

    @pytest.mark.asyncio
    async def test_run_query_job_periodic_heartbeat_for_slow_job(self, monkeypatch):
        from unittest.mock import AsyncMock, patch

        store = QueryJobStore(ttl_seconds=3600.0)
        job = await store.create()

        async def _slow_query(_):
            await asyncio.sleep(0.05)
            return {
                "answer": "test", "sources": [],
                "complexity": "entity_lookup", "retrieval_path": "vector",
            }

        monkeypatch.setenv("ASYNC_JOB_HEARTBEAT_INTERVAL_SECONDS", "0.01")
        with patch("orchestrator.app.main._JOB_STORE", store), \
             patch("orchestrator.app.main.query_graph") as mg:
            mg.ainvoke = _slow_query
            store.heartbeat = AsyncMock()
            from orchestrator.app.main import _run_query_job
            await _run_query_job(job.job_id, {"query": "test"})
            assert store.heartbeat.await_count >= 2

    @pytest.mark.asyncio
    async def test_run_ingest_job_periodic_heartbeat_for_slow_job(self, monkeypatch):
        from unittest.mock import AsyncMock, patch

        store = IngestJobStore(ttl_seconds=3600.0)
        job = await store.create()
        mock_sem = AsyncMock()
        mock_sem.try_acquire = AsyncMock(return_value=(True, "tok"))
        mock_sem.release = AsyncMock()

        async def _slow_ingest(_):
            await asyncio.sleep(0.05)
            return {
                "commit_status": "success", "extracted_nodes": [],
                "extraction_errors": [],
            }

        monkeypatch.setenv("ASYNC_JOB_HEARTBEAT_INTERVAL_SECONDS", "0.01")
        with patch("orchestrator.app.main._INGEST_JOB_STORE", store), \
             patch("orchestrator.app.main.ingestion_graph") as mg, \
             patch("orchestrator.app.main.get_ingestion_semaphore", return_value=mock_sem):
            mg.ainvoke = _slow_ingest
            store.heartbeat = AsyncMock()
            from orchestrator.app.main import _run_ingest_job
            await _run_ingest_job(job.job_id, [])
            assert store.heartbeat.await_count >= 2
