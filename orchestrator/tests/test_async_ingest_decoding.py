from __future__ import annotations

import asyncio
import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
class TestAsyncBase64Decoding:

    async def test_ingest_decodes_in_thread_pool(self) -> None:
        from orchestrator.app.main import _decode_documents_async

        content = base64.b64encode(b"package main").decode()
        mock_request = MagicMock()
        mock_request.documents = [
            MagicMock(file_path="main.go", content=content),
        ]

        result = await _decode_documents_async(mock_request)
        assert len(result) == 1
        assert result[0]["path"] == "main.go"
        assert result[0]["content"] == "package main"

    async def test_decode_async_does_not_block_event_loop(self) -> None:
        from orchestrator.app.main import _decode_documents_async

        content = base64.b64encode(b"x" * 1024).decode()
        mock_request = MagicMock()
        mock_request.documents = [
            MagicMock(file_path="big.bin", content=content),
        ]

        loop_blocked = False
        original_to_thread = asyncio.to_thread

        async def tracking_to_thread(fn, *args, **kwargs):
            nonlocal loop_blocked
            loop_blocked = True
            return await original_to_thread(fn, *args, **kwargs)

        with patch("orchestrator.app.main.asyncio") as mock_asyncio:
            mock_asyncio.to_thread = tracking_to_thread
            mock_asyncio.create_task = asyncio.create_task
            await _decode_documents_async(mock_request)

        assert loop_blocked, (
            "_decode_documents_async must use asyncio.to_thread "
            "to avoid blocking the event loop during base64 decoding"
        )
