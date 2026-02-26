from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.main import _ingest_sync


class TestSyncIngestTimeout:

    @pytest.mark.asyncio
    async def test_slow_ingestion_returns_504(self) -> None:
        async def _stall_forever(_raw_files):
            await asyncio.sleep(9999)
            return {"commit_status": "success", "extracted_nodes": [], "extraction_errors": []}

        with (
            patch.dict("os.environ", {"INGEST_SYNC_TIMEOUT_SECONDS": "0.1"}),
            patch(
                "orchestrator.app.main._invoke_ingestion_graph",
                side_effect=_stall_forever,
            ),
            patch(
                "orchestrator.app.main.get_ingestion_semaphore",
                return_value=asyncio.Semaphore(1),
            ),
        ):
            from fastapi import HTTPException
            with pytest.raises(HTTPException) as exc_info:
                await _ingest_sync([{"path": "test.go", "content": "package main"}])
            assert exc_info.value.status_code == 504

    @pytest.mark.asyncio
    async def test_fast_ingestion_succeeds_within_timeout(self) -> None:
        mock_result = {
            "commit_status": "success",
            "extracted_nodes": ["node1"],
            "extraction_errors": [],
        }

        with (
            patch.dict("os.environ", {"INGEST_SYNC_TIMEOUT_SECONDS": "10"}),
            patch(
                "orchestrator.app.main._invoke_ingestion_graph",
                new_callable=AsyncMock,
                return_value=mock_result,
            ),
            patch(
                "orchestrator.app.main.get_ingestion_semaphore",
                return_value=asyncio.Semaphore(1),
            ),
        ):
            response = await _ingest_sync(
                [{"path": "test.go", "content": "package main"}],
            )
            assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_default_timeout_is_120_seconds(self) -> None:
        import os
        os.environ.pop("INGEST_SYNC_TIMEOUT_SECONDS", None)

        mock_result = {
            "commit_status": "success",
            "extracted_nodes": [],
            "extraction_errors": [],
        }

        with (
            patch(
                "orchestrator.app.main._invoke_ingestion_graph",
                new_callable=AsyncMock,
                return_value=mock_result,
            ),
            patch(
                "orchestrator.app.main.get_ingestion_semaphore",
                return_value=asyncio.Semaphore(1),
            ),
        ):
            from orchestrator.app.main import _get_sync_ingest_timeout
            timeout = _get_sync_ingest_timeout()
            assert timeout == 120.0
