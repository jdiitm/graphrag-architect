from __future__ import annotations

import json
import logging
import uuid
from collections import OrderedDict
from typing import Any, List, Protocol, runtime_checkable

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)



class VectorSyncEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    collection: str
    operation: str = "delete"
    pruned_ids: List[str] = Field(default_factory=list)
    vectors: List[Any] = Field(default_factory=list)
    status: str = "pending"
    retry_count: int = 0

    @field_validator("pruned_ids")
    @classmethod
    def _require_nonempty_ids_for_delete(cls, v: List[str], info: Any) -> List[str]:
        if info.data.get("operation", "delete") == "delete" and not v:
            raise ValueError("pruned_ids must contain at least one ID for delete operations")
        return v


@runtime_checkable
class VectorDeleter(Protocol):
    async def delete(self, collection: str, ids: List[str]) -> int: ...


@runtime_checkable
class OutboxStore(Protocol):
    async def write_event(self, event: VectorSyncEvent) -> None: ...
    async def load_pending(self) -> List[VectorSyncEvent]: ...
    async def delete_event(self, event_id: str) -> None: ...
    async def update_retry_count(
        self, event_id: str, retry_count: int,
    ) -> None: ...


class VectorSyncOutbox:
    def __init__(self) -> None:
        self._pending: OrderedDict[str, VectorSyncEvent] = OrderedDict()

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def enqueue(self, event: VectorSyncEvent) -> None:
        self._pending[event.event_id] = event

    def drain_pending(self) -> List[VectorSyncEvent]:
        return list(self._pending.values())

    def mark_emitted(self, event_id: str) -> None:
        self._pending.pop(event_id, None)


class OutboxDrainer:
    def __init__(
        self,
        outbox: VectorSyncOutbox,
        vector_store: Any,
    ) -> None:
        self._outbox = outbox
        self._vector_store = vector_store

    async def process_once(self) -> int:
        pending = self._outbox.drain_pending()
        if not pending:
            return 0

        processed = 0
        for event in pending:
            try:
                if event.operation == "upsert":
                    await self._vector_store.upsert(
                        event.collection, event.vectors,
                    )
                else:
                    await self._vector_store.delete(
                        event.collection, event.pruned_ids,
                    )
                self._outbox.mark_emitted(event.event_id)
                processed += 1
            except Exception as exc:
                logger.warning(
                    "Vector sync failed for event %s: %s",
                    event.event_id, exc,
                )
        return processed


class RedisOutboxStore:
    _KEY_PREFIX = "graphrag:vecoutbox:"
    _INDEX_KEY = "graphrag:vecoutbox:pending"

    def __init__(self, redis_conn: Any) -> None:
        self._redis = redis_conn

    def _event_key(self, event_id: str) -> str:
        return f"{self._KEY_PREFIX}{event_id}"

    async def write_event(self, event: VectorSyncEvent) -> None:
        mapping = {
            "event_id": event.event_id,
            "collection": event.collection,
            "pruned_ids": json.dumps(event.pruned_ids),
            "status": event.status,
            "retry_count": str(event.retry_count),
        }
        await self._redis.hset(
            self._event_key(event.event_id), mapping=mapping,
        )
        await self._redis.sadd(self._INDEX_KEY, event.event_id)

    async def load_pending(self) -> List[VectorSyncEvent]:
        event_ids = await self._redis.smembers(self._INDEX_KEY)
        events: List[VectorSyncEvent] = []
        for eid in event_ids:
            data = await self._redis.hgetall(self._event_key(eid))
            if not data:
                await self._redis.srem(self._INDEX_KEY, eid)
                continue
            events.append(VectorSyncEvent(
                event_id=data["event_id"],
                collection=data["collection"],
                pruned_ids=json.loads(data["pruned_ids"]),
                status=data.get("status", "pending"),
                retry_count=int(data.get("retry_count", "0")),
            ))
        return events

    async def delete_event(self, event_id: str) -> None:
        await self._redis.delete(self._event_key(event_id))
        await self._redis.srem(self._INDEX_KEY, event_id)

    async def update_retry_count(
        self, event_id: str, retry_count: int,
    ) -> None:
        await self._redis.hset(
            self._event_key(event_id),
            key="retry_count",
            value=str(retry_count),
        )


DEFAULT_MAX_RETRIES = 5


class DurableOutboxDrainer:
    def __init__(
        self,
        store: OutboxStore,
        vector_store: Any,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        self._store = store
        self._vector_store = vector_store
        self._max_retries = max_retries

    async def process_once(self) -> int:
        pending = await self._store.load_pending()
        if not pending:
            return 0

        processed = 0
        for event in pending:
            try:
                if event.operation == "upsert":
                    await self._vector_store.upsert(
                        event.collection, event.vectors,
                    )
                else:
                    await self._vector_store.delete(
                        event.collection, event.pruned_ids,
                    )
                await self._store.delete_event(event.event_id)
                processed += 1
            except Exception as exc:
                new_count = event.retry_count + 1
                if new_count >= self._max_retries:
                    logger.error(
                        "Vector sync event %s exceeded max retries (%d), "
                        "discarding: %s",
                        event.event_id, self._max_retries, exc,
                    )
                    await self._store.delete_event(event.event_id)
                else:
                    logger.warning(
                        "Vector sync failed for event %s (retry %d/%d): %s",
                        event.event_id, new_count, self._max_retries, exc,
                    )
                    await self._store.update_retry_count(
                        event.event_id, new_count,
                    )
        return processed
