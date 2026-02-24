from __future__ import annotations

import os
import time
from typing import Protocol


class CompletionStore(Protocol):
    async def mark(self, content_hash: str) -> None: ...
    async def exists(self, content_hash: str) -> bool: ...
    async def cleanup(self, max_age_seconds: int) -> int: ...


class FileCompletionStore:
    def __init__(self, directory: str) -> None:
        self._dir = directory
        os.makedirs(directory, exist_ok=True)

    def _path(self, content_hash: str) -> str:
        safe_hash = content_hash.replace("/", "_").replace("..", "_")
        return os.path.join(self._dir, f"{safe_hash}.complete")

    async def mark(self, content_hash: str) -> None:
        path = self._path(content_hash)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(str(time.time()))

    async def exists(self, content_hash: str) -> bool:
        return os.path.exists(self._path(content_hash))

    async def cleanup(self, max_age_seconds: int = 86400) -> int:
        cutoff = time.time() - max_age_seconds
        removed = 0
        for entry in os.listdir(self._dir):
            if not entry.endswith(".complete"):
                continue
            full_path = os.path.join(self._dir, entry)
            try:
                mtime = os.path.getmtime(full_path)
                if mtime < cutoff:
                    os.remove(full_path)
                    removed += 1
            except OSError:
                continue
        return removed


class CompletionTracker:
    def __init__(self, store: CompletionStore) -> None:
        self._store = store

    async def mark_committed(self, content_hash: str) -> None:
        await self._store.mark(content_hash)

    async def is_committed(self, content_hash: str) -> bool:
        return await self._store.exists(content_hash)

    async def should_skip(self, content_hash: str) -> bool:
        return await self._store.exists(content_hash)
