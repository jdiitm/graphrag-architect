from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from typing import Any, Dict, List, Optional, Protocol


class TopologyCommitter(Protocol):
    async def commit_topology(self, nodes: List[Any]) -> None: ...


class MessageProducer(Protocol):
    async def send(
        self, topic: str, value: bytes, key: Optional[bytes] = None,
    ) -> None: ...


def _get_partition_key(entity: Any) -> str:
    namespace_acl = getattr(entity, "namespace_acl", None)
    if namespace_acl and isinstance(namespace_acl, list) and namespace_acl:
        return namespace_acl[0]
    team = getattr(entity, "team_owner", None)
    if team:
        return team
    return "_default"


class IncrementalNodeSink:
    def __init__(
        self,
        committer: TopologyCommitter,
        batch_size: int = 500,
        parallel_partitions: bool = False,
    ) -> None:
        self._committer = committer
        self._batch_size = batch_size
        self._parallel = parallel_partitions
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
            if self._parallel:
                await self._commit_partitioned(batch)
            else:
                await self._committer.commit_topology(batch)
            self._flush_count += 1

    async def flush(self) -> None:
        if self._buffer:
            if self._parallel:
                await self._commit_partitioned(list(self._buffer))
            else:
                await self._committer.commit_topology(list(self._buffer))
            self._flush_count += 1
            self._buffer.clear()

    async def _commit_partitioned(self, batch: List[Any]) -> None:
        partitions: Dict[str, List[Any]] = defaultdict(list)
        for entity in batch:
            key = _get_partition_key(entity)
            partitions[key].append(entity)
        if len(partitions) <= 1:
            await self._committer.commit_topology(batch)
            return
        tasks = [
            self._committer.commit_topology(partition_batch)
            for partition_batch in partitions.values()
        ]
        await asyncio.gather(*tasks)


class DurableNodeSink:
    def __init__(
        self,
        committer: TopologyCommitter,
        producer: MessageProducer,
        topic: str = "entity-outbox",
        batch_size: int = 500,
    ) -> None:
        self._inner = IncrementalNodeSink(committer, batch_size)
        self._producer = producer
        self._topic = topic

    @property
    def total_entities(self) -> int:
        return self._inner.total_entities

    @property
    def flush_count(self) -> int:
        return self._inner.flush_count

    async def ingest(self, nodes: List[Any]) -> None:
        payload = json.dumps(
            [_serialize_entity(n) for n in nodes], default=str,
        ).encode("utf-8")
        await self._producer.send(self._topic, value=payload)
        await self._inner.ingest(nodes)

    async def flush(self) -> None:
        await self._inner.flush()


def _serialize_entity(entity: Any) -> Any:
    if hasattr(entity, "model_dump"):
        return entity.model_dump()
    if hasattr(entity, "__dict__"):
        return entity.__dict__
    return str(entity)
