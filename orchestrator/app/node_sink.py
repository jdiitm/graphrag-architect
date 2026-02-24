from __future__ import annotations

from typing import Any, List, Protocol


class TopologyCommitter(Protocol):
    async def commit_topology(self, nodes: List[Any]) -> None: ...


class IncrementalNodeSink:
    def __init__(
        self,
        committer: TopologyCommitter,
        batch_size: int = 500,
    ) -> None:
        self._committer = committer
        self._batch_size = batch_size
        self._buffer: List[Any] = []
        self._total_entities = 0
        self._flush_count = 0

    @property
    def total_entities(self) -> int:
        return self._total_entities

    @property
    def flush_count(self) -> int:
        return self._flush_count

    async def ingest(self, nodes: List[Any]) -> None:
        self._buffer.extend(nodes)
        self._total_entities += len(nodes)
        while len(self._buffer) >= self._batch_size:
            batch = self._buffer[:self._batch_size]
            self._buffer = self._buffer[self._batch_size:]
            await self._committer.commit_topology(batch)
            self._flush_count += 1

    async def flush(self) -> None:
        if self._buffer:
            await self._committer.commit_topology(list(self._buffer))
            self._flush_count += 1
            self._buffer.clear()
