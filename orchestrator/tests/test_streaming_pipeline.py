import os

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
    def test_load_workspace_files_uses_chunked_for_directory(self, tmp_path):
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
        result = load_workspace_files(state)
        assert len(result["raw_files"]) == 5


class TestConfigurableChunkSize:
    def test_workspace_max_bytes_from_env(self, tmp_path, monkeypatch):
        for i in range(20):
            f = tmp_path / f"file{i}.go"
            f.write_text("x" * 200, encoding="utf-8")

        monkeypatch.setenv("WORKSPACE_MAX_BYTES", "1000")
        from orchestrator.app.workspace_loader import load_directory_chunked
        chunks = list(load_directory_chunked(
            str(tmp_path),
            chunk_size=50,
            max_total_bytes=int(os.environ.get("WORKSPACE_MAX_BYTES", "104857600")),
        ))
        total_files = sum(len(c) for c in chunks)
        assert total_files < 20
