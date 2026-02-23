import uuid

import pytest

from orchestrator.app.checkpointing import (
    ExtractionCheckpoint,
    FileStatus,
)


class TestExtractionCheckpoint:
    def test_new_checkpoint_all_pending(self):
        files = [
            {"path": "main.go", "content": "package main"},
            {"path": "app.py", "content": "print('hi')"},
        ]
        cp = ExtractionCheckpoint.from_files(files)
        assert cp.status("main.go") == FileStatus.PENDING
        assert cp.status("app.py") == FileStatus.PENDING

    def test_mark_extracted(self):
        cp = ExtractionCheckpoint.from_files([
            {"path": "main.go", "content": "package main"},
        ])
        cp.mark(["main.go"], FileStatus.EXTRACTED)
        assert cp.status("main.go") == FileStatus.EXTRACTED

    def test_mark_failed(self):
        cp = ExtractionCheckpoint.from_files([
            {"path": "main.go", "content": "package main"},
        ])
        cp.mark(["main.go"], FileStatus.FAILED)
        assert cp.status("main.go") == FileStatus.FAILED

    def test_pending_files_returns_only_pending(self):
        cp = ExtractionCheckpoint.from_files([
            {"path": "a.go", "content": ""},
            {"path": "b.go", "content": ""},
            {"path": "c.go", "content": ""},
        ])
        cp.mark(["a.go"], FileStatus.EXTRACTED)
        cp.mark(["c.go"], FileStatus.FAILED)
        assert cp.pending_paths() == ["b.go"]

    def test_failed_files_returns_only_failed(self):
        cp = ExtractionCheckpoint.from_files([
            {"path": "a.go", "content": ""},
            {"path": "b.go", "content": ""},
            {"path": "c.go", "content": ""},
        ])
        cp.mark(["a.go"], FileStatus.EXTRACTED)
        cp.mark(["b.go"], FileStatus.FAILED)
        assert cp.failed_paths() == ["b.go"]

    def test_filter_files_by_status(self):
        files = [
            {"path": "a.go", "content": "a"},
            {"path": "b.go", "content": "b"},
            {"path": "c.go", "content": "c"},
        ]
        cp = ExtractionCheckpoint.from_files(files)
        cp.mark(["a.go"], FileStatus.EXTRACTED)
        filtered = cp.filter_files(files, FileStatus.PENDING)
        assert len(filtered) == 2
        paths = [f["path"] for f in filtered]
        assert "a.go" not in paths

    def test_retry_resets_failed_to_pending(self):
        cp = ExtractionCheckpoint.from_files([
            {"path": "a.go", "content": ""},
        ])
        cp.mark(["a.go"], FileStatus.FAILED)
        cp.retry_failed()
        assert cp.status("a.go") == FileStatus.PENDING

    def test_all_done_when_no_pending_or_failed(self):
        cp = ExtractionCheckpoint.from_files([
            {"path": "a.go", "content": ""},
            {"path": "b.go", "content": ""},
        ])
        cp.mark(["a.go", "b.go"], FileStatus.EXTRACTED)
        assert cp.all_done is True

    def test_not_all_done_when_pending_remain(self):
        cp = ExtractionCheckpoint.from_files([
            {"path": "a.go", "content": ""},
            {"path": "b.go", "content": ""},
        ])
        cp.mark(["a.go"], FileStatus.EXTRACTED)
        assert cp.all_done is False

    def test_serialization_roundtrip(self):
        cp = ExtractionCheckpoint.from_files([
            {"path": "a.go", "content": ""},
            {"path": "b.go", "content": ""},
        ])
        cp.mark(["a.go"], FileStatus.EXTRACTED)
        data = cp.to_dict()
        restored = ExtractionCheckpoint.from_dict(data)
        assert restored.status("a.go") == FileStatus.EXTRACTED
        assert restored.status("b.go") == FileStatus.PENDING

    def test_checkpoint_has_uuid_id(self):
        cp = ExtractionCheckpoint.from_files([
            {"path": "a.go", "content": ""},
        ])
        assert cp.checkpoint_id is not None
        uuid.UUID(cp.checkpoint_id)

    def test_checkpoint_id_preserved_in_serialization(self):
        cp = ExtractionCheckpoint.from_files([
            {"path": "a.go", "content": ""},
        ])
        original_id = cp.checkpoint_id
        data = cp.to_dict()
        restored = ExtractionCheckpoint.from_dict(data)
        assert restored.checkpoint_id == original_id

    def test_different_checkpoints_have_unique_ids(self):
        cp1 = ExtractionCheckpoint.from_files([{"path": "a.go", "content": ""}])
        cp2 = ExtractionCheckpoint.from_files([{"path": "b.go", "content": ""}])
        assert cp1.checkpoint_id != cp2.checkpoint_id

    def test_save_and_load_from_disk(self, tmp_path):
        cp = ExtractionCheckpoint.from_files([
            {"path": "a.go", "content": ""},
            {"path": "b.go", "content": ""},
        ])
        cp.mark(["a.go"], FileStatus.EXTRACTED)
        cp.mark(["b.go"], FileStatus.FAILED)

        filepath = tmp_path / "checkpoint.json"
        cp.save(filepath)

        loaded = ExtractionCheckpoint.load(filepath)
        assert loaded.checkpoint_id == cp.checkpoint_id
        assert loaded.status("a.go") == FileStatus.EXTRACTED
        assert loaded.status("b.go") == FileStatus.FAILED

    def test_load_nonexistent_file_returns_none(self, tmp_path):
        filepath = tmp_path / "missing.json"
        result = ExtractionCheckpoint.load(filepath)
        assert result is None

    def test_all_done_with_non_source_files(self):
        files = [
            {"path": "main.go", "content": "package main"},
            {"path": "deploy.yaml", "content": "apiVersion: v1"},
            {"path": "Dockerfile", "content": "FROM alpine"},
        ]
        cp = ExtractionCheckpoint.from_files(files)
        cp.mark(["main.go"], FileStatus.EXTRACTED)
        assert cp.all_done is True

    def test_non_source_files_marked_skipped(self):
        files = [
            {"path": "deploy.yaml", "content": "apiVersion: v1"},
            {"path": "main.go", "content": "package main"},
        ]
        cp = ExtractionCheckpoint.from_files(files)
        assert cp.status("deploy.yaml") == FileStatus.SKIPPED
