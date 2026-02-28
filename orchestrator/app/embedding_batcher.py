from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


class RateLimitError(Exception):
    pass


@dataclass(frozen=True)
class EmbeddingBatcherConfig:
    max_batch_size: int = 2048
    flush_interval_ms: int = 500
    max_retries: int = 3
    base_backoff_ms: float = 100.0
    max_backoff_ms: float = 10000.0


@runtime_checkable
class EmbeddingProvider(Protocol):
    async def embed_batch(self, texts: List[str]) -> List[List[float]]: ...


@dataclass
class _PendingItem:
    text: str
    metadata: Dict[str, Any]
    future: asyncio.Future[List[float]]


class EmbeddingBatcher:
    def __init__(
        self,
        provider: Any,
        config: Optional[EmbeddingBatcherConfig] = None,
    ) -> None:
        self._provider = provider
        self._config = config or EmbeddingBatcherConfig()
        self._queue: asyncio.Queue[_PendingItem] = asyncio.Queue()
        self._flush_task: Optional[asyncio.Task[None]] = None
        self._closed = False

    async def start(self) -> None:
        self._closed = False
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def close(self) -> None:
        self._closed = True
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None
        await self._drain_remaining()

    async def __aenter__(self) -> EmbeddingBatcher:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        await self.close()

    def submit(
        self, text: str, metadata: Dict[str, Any],
    ) -> asyncio.Future[List[float]]:
        if self._closed:
            raise RuntimeError("Cannot submit to a closed batcher")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[List[float]] = loop.create_future()
        item = _PendingItem(text=text, metadata=metadata, future=future)
        self._queue.put_nowait(item)
        return future

    async def _flush_loop(self) -> None:
        interval_seconds = self._config.flush_interval_ms / 1000.0
        while True:
            batch = await self._collect_batch(interval_seconds)
            if batch:
                await self._send_batch(batch)

    async def _collect_batch(
        self, timeout: float,
    ) -> List[_PendingItem]:
        batch: List[_PendingItem] = []
        try:
            first = await asyncio.wait_for(
                self._queue.get(), timeout=timeout,
            )
            batch.append(first)
        except asyncio.TimeoutError:
            return batch

        while len(batch) < self._config.max_batch_size:
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        if len(batch) >= self._config.max_batch_size:
            return batch

        try:
            batch.append(
                await asyncio.wait_for(self._queue.get(), timeout=timeout),
            )
        except asyncio.TimeoutError:
            pass

        return batch

    async def _send_batch(self, batch: List[_PendingItem]) -> None:
        texts = [item.text for item in batch]
        last_error: Optional[Exception] = None
        for attempt in range(self._config.max_retries + 1):
            try:
                embeddings = await self._provider.embed_batch(texts)
                for item, embedding in zip(batch, embeddings):
                    if not item.future.done():
                        item.future.set_result(embedding)
                return
            except RateLimitError as exc:
                last_error = exc
                if attempt < self._config.max_retries:
                    backoff = min(
                        self._config.base_backoff_ms * (2 ** attempt),
                        self._config.max_backoff_ms,
                    )
                    jitter = random.uniform(0, backoff * 0.1)
                    await asyncio.sleep((backoff + jitter) / 1000.0)
                    continue
                break

        if last_error is not None:
            for item in batch:
                if not item.future.done():
                    item.future.set_exception(last_error)

    async def _drain_remaining(self) -> None:
        remaining: List[_PendingItem] = []
        while not self._queue.empty():
            try:
                item = self._queue.get_nowait()
                remaining.append(item)
            except asyncio.QueueEmpty:
                break
        if remaining:
            for i in range(0, len(remaining), self._config.max_batch_size):
                chunk = remaining[i : i + self._config.max_batch_size]
                await self._send_batch(chunk)
