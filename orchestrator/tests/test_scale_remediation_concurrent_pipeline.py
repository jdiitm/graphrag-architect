from __future__ import annotations

import asyncio
import os
from unittest.mock import AsyncMock, patch

import pytest

_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


@pytest.mark.asyncio
class TestConcurrentPipelineReturnExceptions:

    async def test_failed_chunk_does_not_abort_others(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        call_count = 0

        async def _alternating(state):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("LLM extraction timeout")
            return {
                "extracted_nodes": [{"id": f"n-{call_count}"}],
                "commit_status": "success",
                "extraction_errors": [],
                "extraction_checkpoint": {},
                "skipped_files": [],
            }

        async def fake_stream(directory_path, **kwargs):
            for i in range(3):
                yield [{"path": f"f{i}.go", "content": f"package f{i}"}]

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.load_directory_stream",
                side_effect=fake_stream,
            ),
            patch(
                "orchestrator.app.graph_builder.ingestion_graph.ainvoke",
                side_effect=_alternating,
            ),
        ):
            pipeline = StreamingIngestionPipeline()
            result = await pipeline.process_directory("/workspace")

        assert result["commit_status"] == "failed"
        assert pipeline.chunk_count == 2

    async def test_all_chunks_fail_marks_status_failed(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        async def _always_fail(state):
            raise ValueError("bad extraction")

        async def fake_stream(directory_path, **kwargs):
            for i in range(2):
                yield [{"path": f"f{i}.go", "content": f"package f{i}"}]

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.load_directory_stream",
                side_effect=fake_stream,
            ),
            patch(
                "orchestrator.app.graph_builder.ingestion_graph.ainvoke",
                side_effect=_always_fail,
            ),
        ):
            pipeline = StreamingIngestionPipeline()
            result = await pipeline.process_directory("/workspace")

        assert result["commit_status"] == "failed"
        assert pipeline.chunk_count == 0
        assert pipeline.total_entities == 0


@pytest.mark.asyncio
class TestConcurrentPipelineSemaphore:

    async def test_concurrency_bounded_by_semaphore(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        max_concurrent = 0
        current_concurrent = 0
        lock = asyncio.Lock()

        async def _tracking_invoke(state):
            nonlocal max_concurrent, current_concurrent
            async with lock:
                current_concurrent += 1
                if current_concurrent > max_concurrent:
                    max_concurrent = current_concurrent
            await asyncio.sleep(0.02)
            async with lock:
                current_concurrent -= 1
            return {
                "extracted_nodes": [{"id": "n"}],
                "commit_status": "success",
                "extraction_errors": [],
                "extraction_checkpoint": {},
                "skipped_files": [],
            }

        async def fake_stream(directory_path, **kwargs):
            for i in range(8):
                yield [{"path": f"f{i}.go", "content": f"package f{i}"}]

        with (
            patch.dict("os.environ", {**_ENV_VARS, "INGESTION_CONCURRENCY": "2"}),
            patch(
                "orchestrator.app.graph_builder.load_directory_stream",
                side_effect=fake_stream,
            ),
            patch(
                "orchestrator.app.graph_builder.ingestion_graph.ainvoke",
                side_effect=_tracking_invoke,
            ),
        ):
            pipeline = StreamingIngestionPipeline()
            await pipeline.process_directory("/workspace")

        assert max_concurrent <= 2
        assert pipeline.chunk_count == 8


@pytest.mark.asyncio
class TestConcurrentProcessFiles:

    async def test_process_files_uses_gather(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        mock_invoke = AsyncMock(return_value={
            "extracted_nodes": [{"id": "n"}],
            "commit_status": "success",
            "extraction_errors": [],
            "extraction_checkpoint": {},
            "skipped_files": [],
        })

        raw_files = [
            {"path": f"f{i}.go", "content": f"package f{i}"}
            for i in range(6)
        ]

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.ingestion_graph.ainvoke",
                mock_invoke,
            ),
        ):
            pipeline = StreamingIngestionPipeline(chunk_size=2)
            result = await pipeline.process_files(raw_files)

        assert mock_invoke.call_count == 3
        assert pipeline.chunk_count == 3
        assert result["commit_status"] == "success"

    async def test_process_files_exception_marks_failed(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        call_count = 0

        async def _fail_second(state):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("boom")
            return {
                "extracted_nodes": [],
                "commit_status": "success",
                "extraction_errors": [],
                "extraction_checkpoint": {},
                "skipped_files": [],
            }

        raw_files = [
            {"path": f"f{i}.go", "content": f"package f{i}"}
            for i in range(4)
        ]

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.ingestion_graph.ainvoke",
                side_effect=_fail_second,
            ),
        ):
            pipeline = StreamingIngestionPipeline(chunk_size=2)
            result = await pipeline.process_files(raw_files)

        assert result["commit_status"] == "failed"


@pytest.mark.asyncio
class TestCollectTaskResults:

    async def test_collect_marks_failed_on_exception(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        with patch.dict("os.environ", _ENV_VARS):
            pipeline = StreamingIngestionPipeline()

        pipeline._collect_task_results([None, RuntimeError("err"), None])
        assert pipeline.commit_status == "failed"

    async def test_collect_preserves_success_on_clean_results(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        with patch.dict("os.environ", _ENV_VARS):
            pipeline = StreamingIngestionPipeline()

        pipeline._collect_task_results([None, None, None])
        assert pipeline.commit_status == "success"
