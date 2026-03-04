from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


def _success_result(node_id: str = "n") -> dict:
    return {
        "extracted_nodes": [{"id": node_id}],
        "commit_status": "success",
        "extraction_errors": [],
        "extraction_checkpoint": {},
        "skipped_files": [],
    }


def _empty_success() -> dict:
    return {
        "extracted_nodes": [],
        "commit_status": "success",
        "extraction_errors": [],
        "extraction_checkpoint": {},
        "skipped_files": [],
    }


@pytest.mark.asyncio
class TestConcurrentPipelineReturnExceptions:

    async def test_failed_chunk_does_not_abort_others(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        invocation_count = 0

        async def _alternating(state):
            nonlocal invocation_count
            invocation_count += 1
            if invocation_count == 2:
                raise RuntimeError("LLM extraction timeout")
            return _success_result(f"n-{invocation_count}")

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

        assert invocation_count == 3, (
            f"All 3 chunks must be attempted, but only {invocation_count} were invoked"
        )
        assert result["commit_status"] == "failed"
        assert pipeline.chunk_count == 2

    async def test_all_chunks_fail_marks_status_failed(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        invocation_count = 0

        async def _always_fail(state):
            nonlocal invocation_count
            invocation_count += 1
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

        assert invocation_count == 2
        assert result["commit_status"] == "failed"
        assert pipeline.chunk_count == 0
        assert pipeline.total_entities == 0


@pytest.mark.asyncio
class TestConcurrentPipelineSemaphore:

    async def test_concurrency_bounded_and_saturated_by_semaphore(self) -> None:
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
            await asyncio.sleep(0.03)
            async with lock:
                current_concurrent -= 1
            return _success_result()

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

        assert pipeline.chunk_count == 8
        assert max_concurrent >= 2, (
            f"Expected semaphore to be saturated (>=2) but max was {max_concurrent}"
        )
        assert max_concurrent <= 2, (
            f"Expected semaphore to cap at 2 but max was {max_concurrent}"
        )

    async def test_chunks_execute_concurrently_not_serially(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        chunk_delay = 0.05

        async def _slow_invoke(state):
            await asyncio.sleep(chunk_delay)
            return _success_result()

        num_chunks = 4
        serial_minimum = chunk_delay * num_chunks

        async def fake_stream(directory_path, **kwargs):
            for i in range(num_chunks):
                yield [{"path": f"f{i}.go", "content": f"package f{i}"}]

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.load_directory_stream",
                side_effect=fake_stream,
            ),
            patch(
                "orchestrator.app.graph_builder.ingestion_graph.ainvoke",
                side_effect=_slow_invoke,
            ),
        ):
            pipeline = StreamingIngestionPipeline()
            start = time.monotonic()
            await pipeline.process_directory("/workspace")
            elapsed = time.monotonic() - start

        assert pipeline.chunk_count == num_chunks
        assert elapsed < serial_minimum, (
            f"Took {elapsed:.3f}s — serial minimum is {serial_minimum:.3f}s. "
            f"Chunks are not running concurrently."
        )


@pytest.mark.asyncio
class TestConcurrentProcessFiles:

    async def test_process_files_dispatches_all_chunks(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        mock_invoke = AsyncMock(return_value=_success_result())

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
        assert pipeline.total_entities == 3
        assert result["commit_status"] == "success"

    async def test_process_files_exception_marks_failed(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        invocation_count = 0

        async def _fail_second(state):
            nonlocal invocation_count
            invocation_count += 1
            if invocation_count == 2:
                raise RuntimeError("boom")
            return _empty_success()

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

        assert invocation_count == 2
        assert result["commit_status"] == "failed"
        assert pipeline.chunk_count == 1


@pytest.mark.asyncio
class TestCollectTaskResults:

    async def test_single_exception_marks_status_failed(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        with patch.dict("os.environ", _ENV_VARS):
            pipeline = StreamingIngestionPipeline()

        assert pipeline.commit_status == "success"
        pipeline._collect_task_results([None, RuntimeError("err"), None])
        assert pipeline.commit_status == "failed"

    async def test_multiple_exceptions_all_trigger_failed(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        with patch.dict("os.environ", _ENV_VARS):
            pipeline = StreamingIngestionPipeline()

        pipeline._collect_task_results([
            ValueError("a"), TypeError("b"), None,
        ])
        assert pipeline.commit_status == "failed"

    async def test_no_exceptions_preserves_success(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        with patch.dict("os.environ", _ENV_VARS):
            pipeline = StreamingIngestionPipeline()

        pipeline._collect_task_results([None, None, None])
        assert pipeline.commit_status == "success"

    async def test_empty_results_preserves_success(self) -> None:
        from orchestrator.app.graph_builder import StreamingIngestionPipeline

        with patch.dict("os.environ", _ENV_VARS):
            pipeline = StreamingIngestionPipeline()

        pipeline._collect_task_results([])
        assert pipeline.commit_status == "success"
