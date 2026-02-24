from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.workspace_loader import load_directory_chunked


class TestStreamingWorkspaceLoader:
    def test_chunked_loader_respects_max_total_bytes(self, tmp_path):
        for i in range(10):
            f = tmp_path / f"file{i}.go"
            f.write_text("x" * 100, encoding="utf-8")

        chunks = list(load_directory_chunked(
            str(tmp_path), chunk_size=3, max_total_bytes=500,
        ))
        total_files = sum(len(c) for c in chunks)
        assert total_files < 10

    def test_chunked_loader_yields_in_batches(self, tmp_path):
        for i in range(7):
            f = tmp_path / f"file{i}.go"
            f.write_text(f"package p{i}", encoding="utf-8")

        chunks = list(load_directory_chunked(str(tmp_path), chunk_size=3))
        assert len(chunks) == 3
        assert len(chunks[0]) == 3
        assert len(chunks[1]) == 3
        assert len(chunks[2]) == 1


class TestLoadWorkspaceUsesChunkedLoader:
    @pytest.mark.asyncio
    async def test_load_workspace_files_uses_chunked_for_directory(self, tmp_path):
        for i in range(5):
            f = tmp_path / f"svc{i}.go"
            f.write_text(f"package main{i}", encoding="utf-8")

        from orchestrator.app.graph_builder import load_workspace_files
        state = {
            "directory_path": str(tmp_path),
            "raw_files": [],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
            "extraction_checkpoint": {},
        }
        result = await load_workspace_files(state)
        assert len(result["raw_files"]) == 5


class TestConfigurableChunkSize:
    def test_workspace_max_bytes_from_env(self, tmp_path, monkeypatch):
        for i in range(20):
            f = tmp_path / f"file{i}.go"
            f.write_text("x" * 200, encoding="utf-8")

        monkeypatch.setenv("WORKSPACE_MAX_BYTES", "1000")
        from orchestrator.app.graph_builder import _get_workspace_max_bytes
        max_bytes = _get_workspace_max_bytes()
        chunks = list(load_directory_chunked(
            str(tmp_path), chunk_size=50, max_total_bytes=max_bytes,
        ))
        total_files = sum(len(c) for c in chunks)
        assert total_files < 20


class TestSkippedFilesTracking:
    def test_skipped_files_populated_when_limit_exceeded(self, tmp_path):
        for i in range(10):
            (tmp_path / f"file{i:02d}.go").write_text("x" * 1000)

        skipped: list[str] = []
        chunks = list(load_directory_chunked(
            str(tmp_path), chunk_size=5, max_total_bytes=3000, skipped=skipped,
        ))
        loaded_count = sum(len(c) for c in chunks)
        assert loaded_count == 3
        assert len(skipped) == 7
        assert loaded_count + len(skipped) == 10

    def test_skipped_files_empty_when_all_fit(self, tmp_path):
        for i in range(3):
            (tmp_path / f"file{i}.go").write_text("short")

        skipped: list[str] = []
        chunks = list(load_directory_chunked(
            str(tmp_path), chunk_size=10, max_total_bytes=100000, skipped=skipped,
        ))
        assert sum(len(c) for c in chunks) == 3
        assert skipped == []

    def test_skipped_files_none_default_no_error(self, tmp_path):
        for i in range(5):
            (tmp_path / f"file{i}.go").write_text("x" * 1000)

        chunks = list(load_directory_chunked(
            str(tmp_path), chunk_size=5, max_total_bytes=2000,
        ))
        loaded = sum(len(c) for c in chunks)
        assert loaded < 5

    @pytest.mark.asyncio
    async def test_load_workspace_files_surfaces_skipped(self, tmp_path):
        for i in range(10):
            (tmp_path / f"file{i:02d}.go").write_text("x" * 1000)

        from orchestrator.app.graph_builder import load_workspace_files

        with patch.dict("os.environ", {"WORKSPACE_MAX_BYTES": "3000"}):
            result = await load_workspace_files({
                "directory_path": str(tmp_path),
                "raw_files": [],
                "extracted_nodes": [],
                "extraction_errors": [],
                "validation_retries": 0,
                "commit_status": "",
                "extraction_checkpoint": {},
            })

        assert "skipped_files" in result
        assert len(result["skipped_files"]) > 0
        assert len(result["raw_files"]) + len(result["skipped_files"]) == 10


class TestWorkspaceMaxBytesValidation:
    def test_invalid_value_raises_with_message(self, monkeypatch):
        monkeypatch.setenv("WORKSPACE_MAX_BYTES", "not_a_number")
        from orchestrator.app.graph_builder import _get_workspace_max_bytes
        with pytest.raises(ValueError, match="WORKSPACE_MAX_BYTES"):
            _get_workspace_max_bytes()

    def test_zero_value_raises(self, monkeypatch):
        monkeypatch.setenv("WORKSPACE_MAX_BYTES", "0")
        from orchestrator.app.graph_builder import _get_workspace_max_bytes
        with pytest.raises(ValueError, match="positive integer"):
            _get_workspace_max_bytes()

    def test_negative_value_raises(self, monkeypatch):
        monkeypatch.setenv("WORKSPACE_MAX_BYTES", "-100")
        from orchestrator.app.graph_builder import _get_workspace_max_bytes
        with pytest.raises(ValueError, match="positive integer"):
            _get_workspace_max_bytes()

    def test_valid_value_returns_int(self, monkeypatch):
        monkeypatch.setenv("WORKSPACE_MAX_BYTES", "5000")
        from orchestrator.app.graph_builder import _get_workspace_max_bytes
        assert _get_workspace_max_bytes() == 5000


_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}



class TestStreamingPipelinePerChunkCommit:
    @pytest.mark.asyncio
    async def test_multiple_chunks_produce_multiple_commits(self):
        from orchestrator.app.graph_builder import run_streaming_pipeline

        mock_chunks = [
            [{"path": f"svc{i}.go", "content": f"package svc{i}"}
             for i in range(3)],
            [{"path": f"svc{i}.go", "content": f"package svc{i}"}
             for i in range(3, 6)],
            [{"path": f"svc{i}.go", "content": f"package svc{i}"}
             for i in range(6, 9)],
        ]

        mock_invoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "commit_status": "success",
            "extraction_errors": [],
            "extraction_checkpoint": {},
            "skipped_files": [],
        })

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.load_directory_chunked",
                return_value=iter(mock_chunks),
            ),
            patch(
                "orchestrator.app.graph_builder.ingestion_graph.ainvoke",
                mock_invoke,
            ),
        ):
            result = await run_streaming_pipeline({
                "directory_path": "/some/workspace",
            })

        assert mock_invoke.call_count == 3
        assert result["commit_status"] == "success"

    @pytest.mark.asyncio
    async def test_single_chunk_produces_single_commit(self):
        from orchestrator.app.graph_builder import run_streaming_pipeline

        mock_chunks = [
            [{"path": "svc0.go", "content": "package svc0"}],
        ]

        mock_invoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "commit_status": "success",
            "extraction_errors": [],
            "extraction_checkpoint": {},
            "skipped_files": [],
        })

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.load_directory_chunked",
                return_value=iter(mock_chunks),
            ),
            patch(
                "orchestrator.app.graph_builder.ingestion_graph.ainvoke",
                mock_invoke,
            ),
        ):
            result = await run_streaming_pipeline({
                "directory_path": "/some/workspace",
            })

        assert mock_invoke.call_count == 1

    @pytest.mark.asyncio
    async def test_failed_chunk_commit_surfaces_in_result(self):
        from orchestrator.app.graph_builder import run_streaming_pipeline

        call_count = 0

        async def alternating_invoke(state):
            nonlocal call_count
            call_count += 1
            status = "failed" if call_count == 2 else "success"
            return {
                "extracted_nodes": [],
                "commit_status": status,
                "extraction_errors": [],
                "extraction_checkpoint": {},
                "skipped_files": [],
            }

        mock_chunks = [
            [{"path": "a.go", "content": "package a"}],
            [{"path": "b.go", "content": "package b"}],
            [{"path": "c.go", "content": "package c"}],
        ]

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.load_directory_chunked",
                return_value=iter(mock_chunks),
            ),
            patch(
                "orchestrator.app.graph_builder.ingestion_graph.ainvoke",
                side_effect=alternating_invoke,
            ),
        ):
            result = await run_streaming_pipeline({
                "directory_path": "/some/workspace",
            })

        assert result["commit_status"] == "failed"

    @pytest.mark.asyncio
    async def test_preloaded_files_bypass_chunked_loader(self):
        from orchestrator.app.graph_builder import run_streaming_pipeline

        mock_invoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "commit_status": "success",
            "extraction_errors": [],
            "extraction_checkpoint": {},
            "skipped_files": [],
        })

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.ingestion_graph.ainvoke",
                mock_invoke,
            ),
        ):
            result = await run_streaming_pipeline({
                "directory_path": "",
                "raw_files": [{"path": "a.go", "content": "package a"}],
            })

        assert mock_invoke.call_count == 1
        assert result["skipped_files"] == []


class TestStreamingPipelineSkippedFiles:
    @pytest.mark.asyncio
    async def test_skipped_files_surfaced_in_result(self):
        from orchestrator.app.graph_builder import run_streaming_pipeline

        mock_chunks = [
            [{"path": "a.go", "content": "package a"}],
        ]

        def fake_chunked(directory_path, **kwargs):
            skipped = kwargs.get("skipped")
            if skipped is not None:
                skipped.extend(["dropped1.go", "dropped2.go"])
            yield from mock_chunks

        mock_invoke = AsyncMock(return_value={
            "extracted_nodes": [],
            "commit_status": "success",
            "extraction_errors": [],
            "extraction_checkpoint": {},
            "skipped_files": [],
        })

        with (
            patch.dict("os.environ", {**_ENV_VARS, "WORKSPACE_MAX_BYTES": "500"}),
            patch(
                "orchestrator.app.graph_builder.load_directory_chunked",
                side_effect=fake_chunked,
            ),
            patch(
                "orchestrator.app.graph_builder.ingestion_graph.ainvoke",
                mock_invoke,
            ),
        ):
            result = await run_streaming_pipeline({
                "directory_path": "/some/workspace",
            })

        assert result["skipped_files"] == ["dropped1.go", "dropped2.go"]
