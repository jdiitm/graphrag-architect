from __future__ import annotations

from enum import Enum
from typing import Dict, List


class FileStatus(Enum):
    PENDING = "pending"
    EXTRACTED = "extracted"
    FAILED = "failed"


class ExtractionCheckpoint:
    def __init__(self, statuses: Dict[str, FileStatus]) -> None:
        self._statuses = dict(statuses)

    @classmethod
    def from_files(cls, files: List[Dict[str, str]]) -> ExtractionCheckpoint:
        return cls({f["path"]: FileStatus.PENDING for f in files})

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> ExtractionCheckpoint:
        return cls({
            path: FileStatus(status_str)
            for path, status_str in data.items()
        })

    def to_dict(self) -> Dict[str, str]:
        return {path: status.value for path, status in self._statuses.items()}

    def status(self, path: str) -> FileStatus:
        return self._statuses.get(path, FileStatus.PENDING)

    def mark(self, paths: List[str], status: FileStatus) -> None:
        for path in paths:
            if path in self._statuses:
                self._statuses[path] = status

    def pending_paths(self) -> List[str]:
        return [p for p, s in self._statuses.items() if s == FileStatus.PENDING]

    def failed_paths(self) -> List[str]:
        return [p for p, s in self._statuses.items() if s == FileStatus.FAILED]

    def filter_files(
        self,
        files: List[Dict[str, str]],
        target_status: FileStatus,
    ) -> List[Dict[str, str]]:
        return [f for f in files if self._statuses.get(f["path"]) == target_status]

    def retry_failed(self) -> None:
        for path in list(self._statuses):
            if self._statuses[path] == FileStatus.FAILED:
                self._statuses[path] = FileStatus.PENDING

    @property
    def all_done(self) -> bool:
        return all(s == FileStatus.EXTRACTED for s in self._statuses.values())
