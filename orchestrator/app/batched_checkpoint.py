from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import Any, Deque, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class BatchedCheckpointSaver:
    def __init__(
        self,
        inner_saver: Any,
        batch_max_size: int = 50,
        flush_interval_ms: int = 500,
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
        async with self._lock:
            self._buffer.append((config, checkpoint, metadata))
            if len(self._buffer) >= self._batch_max_size:
                await self._flush_locked()

    async def aget_tuple(self, config: Dict[str, Any]) -> Any:
        return await self._inner.aget_tuple(config)

    async def _flush_locked(self) -> None:
        while self._buffer:
            config, checkpoint, metadata = self._buffer.popleft()
            await self._inner.aput(config, checkpoint, metadata)

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
                await self.flush()

    async def close(self) -> None:
        self._closed = True
        if self._timer_task is not None:
            self._timer_task.cancel()
            try:
                await self._timer_task
            except asyncio.CancelledError:
                pass
            self._timer_task = None
        await self.flush()
