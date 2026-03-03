from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import HTTPException

from orchestrator.app.tenant_isolation import TenantContext


@pytest.mark.asyncio
async def test_get_query_job_denies_cross_tenant_access() -> None:
    from orchestrator.app.main import _JOB_STORE, get_query_job

    job = await _JOB_STORE.create(tenant_id="tenant-a")
    with patch(
        "orchestrator.app.main._resolve_tenant_context",
        return_value=TenantContext.default(tenant_id="tenant-b"),
    ):
        with pytest.raises(HTTPException) as exc:
            await get_query_job(job.job_id, authorization="Bearer tenant-b")
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_get_ingest_job_denies_cross_tenant_access() -> None:
    from orchestrator.app.main import _INGEST_JOB_STORE, get_ingest_job

    job = await _INGEST_JOB_STORE.create(tenant_id="tenant-a")
    with patch(
        "orchestrator.app.main._resolve_tenant_context",
        return_value=TenantContext.default(tenant_id="tenant-b"),
    ):
        with pytest.raises(HTTPException) as exc:
            await get_ingest_job(job.job_id, authorization="Bearer tenant-b")
    assert exc.value.status_code == 404


def test_schedule_background_task_enforces_fanout_limit() -> None:
    from orchestrator.app.main import _schedule_background_task

    async def _noop() -> None:
        return None

    tasks = {object()}
    coro = _noop()
    with patch("orchestrator.app.main._ASYNC_JOB_MAX_INFLIGHT", 1):
        with pytest.raises(HTTPException) as exc:
            _schedule_background_task(tasks, coro, "query")
    assert exc.value.status_code == 429
    coro.close()
