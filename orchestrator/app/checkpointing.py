from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from enum import Enum
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)

_CHECKPOINT_ID_KEY = "__checkpoint_id__"


_SOURCE_EXTENSIONS = frozenset({".go", ".py"})


class FileStatus(Enum):
    PENDING = "pending"
    EXTRACTED = "extracted"
    FAILED = "failed"
    SKIPPED = "skipped"


class ExtractionCheckpoint:
    def __init__(
        self,
        statuses: Dict[str, FileStatus],
        checkpoint_id: Optional[str] = None,
    ) -> None:
        self._statuses = dict(statuses)
        self._checkpoint_id = checkpoint_id or str(uuid.uuid4())

    @property
    def checkpoint_id(self) -> str:
        return self._checkpoint_id

    @classmethod
    def from_files(cls, files: List[Dict[str, str]]) -> ExtractionCheckpoint:
        statuses: Dict[str, FileStatus] = {}
        for f in files:
            path = f["path"]
            ext = Path(path).suffix.lower()
            statuses[path] = (
                FileStatus.PENDING if ext in _SOURCE_EXTENSIONS
                else FileStatus.SKIPPED
            )
        return cls(statuses)

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> ExtractionCheckpoint:
        checkpoint_id = data.get(_CHECKPOINT_ID_KEY)
        return cls(
            {
                path: FileStatus(status_str)
                for path, status_str in data.items()
                if path != _CHECKPOINT_ID_KEY
            },
            checkpoint_id=checkpoint_id,
        )

    def to_dict(self) -> Dict[str, str]:
        result = {path: status.value for path, status in self._statuses.items()}
        result[_CHECKPOINT_ID_KEY] = self._checkpoint_id
        return result

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

    def save(self, filepath: Union[str, Path]) -> None:
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")

    @classmethod
    def load(cls, filepath: Union[str, Path]) -> Optional[ExtractionCheckpoint]:
        path = Path(filepath)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return cls.from_dict(data)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load checkpoint from %s: %s", filepath, exc)
            return None

    @property
    def all_done(self) -> bool:
        terminal = {FileStatus.EXTRACTED, FileStatus.SKIPPED}
        return all(s in terminal for s in self._statuses.values())
