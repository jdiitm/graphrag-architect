from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Protocol


@dataclass
class IngestionStatus:
    thread_id: str
    state: str = "running"
    total_files: int = 0
    processed_files: int = 0
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None


class IngestionStatusStore(Protocol):
    async def create(self, thread_id: str, total_files: int) -> None: ...
    async def get(self, thread_id: str) -> Optional[IngestionStatus]: ...
    async def update_progress(
        self, thread_id: str, processed: int,
    ) -> None: ...
    async def mark_completed(self, thread_id: str) -> None: ...
    async def mark_failed(
        self, thread_id: str, error: str,
    ) -> None: ...
    async def cleanup(self, max_age_seconds: int) -> int: ...
    async def list_resumable(self) -> List[IngestionStatus]: ...


class InMemoryStatusStore:
    def __init__(self) -> None:
        self._store: Dict[str, IngestionStatus] = {}

    async def create(self, thread_id: str, total_files: int) -> None:
        self._store[thread_id] = IngestionStatus(
            thread_id=thread_id, total_files=total_files,
        )

    async def get(self, thread_id: str) -> Optional[IngestionStatus]:
        return self._store.get(thread_id)

    async def update_progress(
        self, thread_id: str, processed: int,
    ) -> None:
        status = self._store.get(thread_id)
        if status is not None:
            status.processed_files = processed

    async def mark_completed(self, thread_id: str) -> None:
        status = self._store.get(thread_id)
        if status is not None:
            status.state = "completed"
            status.completed_at = time.time()

    async def mark_failed(self, thread_id: str, error: str) -> None:
        status = self._store.get(thread_id)
        if status is not None:
            status.state = "failed"
            status.error = error
            status.completed_at = time.time()

    async def cleanup(self, max_age_seconds: int = 86400) -> int:
        cutoff = time.time() - max_age_seconds
        to_remove = [
            tid for tid, s in self._store.items()
            if s.completed_at is not None and s.completed_at < cutoff
        ]
        for tid in to_remove:
            del self._store[tid]
        return len(to_remove)

    async def list_resumable(self) -> List[IngestionStatus]:
        return [
            s for s in self._store.values()
            if s.state in ("running", "failed")
        ]
