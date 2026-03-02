from __future__ import annotations

import asyncio
from collections import deque
from typing import Any, Deque, Dict, Optional, Tuple

_DEFAULT_BATCH_MAX_SIZE = 50
_DEFAULT_FLUSH_INTERVAL_MS = 500


class BatchedCheckpointSaver:
    def __init__(
        self,
        inner_saver: Any,
        batch_max_size: int = _DEFAULT_BATCH_MAX_SIZE,
        flush_interval_ms: int = _DEFAULT_FLUSH_INTERVAL_MS,
    ) -> None:
        self._inner = inner_saver
        self._batch_max_size = batch_max_size
        self._flush_interval_ms = flush_interval_ms
        self._buffer: Deque[Tuple[Dict[str, Any], Any, Any]] = deque()
        self._lock = asyncio.Lock()
        self._timer_task: Optional[asyncio.Task[None]] = None
        self._closed = False

    @property
    def pending_count(self) -> int:
        return len(self._buffer)

    async def aput(
        self,
        config: Dict[str, Any],
        checkpoint: Any,
        metadata: Any,
    ) -> None:
        if self._closed:
            raise RuntimeError(
                "Cannot write to a closed BatchedCheckpointSaver"
            )
        async with self._lock:
            self._buffer.append((config, checkpoint, metadata))
            if len(self._buffer) >= self._batch_max_size:
                await self._flush_locked()

    async def aget_tuple(self, config: Dict[str, Any]) -> Any:
        return await self._inner.aget_tuple(config)

    async def _flush_locked(self) -> None:
        failed: Deque[Tuple[Dict[str, Any], Any, Any]] = deque()
        err: Optional[BaseException] = None
        while self._buffer:
            entry = self._buffer.popleft()
            try:
                await self._inner.aput(entry[0], entry[1], entry[2])
            except Exception as exc:
                failed.append(entry)
                err = exc
                break
        if failed:
            failed.extend(self._buffer)
            self._buffer = failed
            raise err  # type: ignore[misc]

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def start_timer(self) -> None:
        if self._timer_task is not None:
            return
        self._timer_task = asyncio.create_task(self._timer_loop())

    async def _timer_loop(self) -> None:
        interval = self._flush_interval_ms / 1000.0
        while not self._closed:
            await asyncio.sleep(interval)
            if self._buffer:
                try:
                    await self.flush()
                except Exception:
                    pass

    async def close(self) -> None:
        self._closed = True
        if self._timer_task is not None:
            self._timer_task.cancel()
            try:
                await self._timer_task
            except asyncio.CancelledError:
                pass
            self._timer_task = None
        async with self._lock:
            await self._flush_locked()
