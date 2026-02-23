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
