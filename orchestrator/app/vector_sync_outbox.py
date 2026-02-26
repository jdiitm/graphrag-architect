from __future__ import annotations

import logging
import uuid
from collections import OrderedDict
from typing import Any, List, Protocol, runtime_checkable

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


class VectorSyncEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    collection: str
    pruned_ids: List[str]
    status: str = "pending"
    retry_count: int = 0

    @field_validator("pruned_ids")
    @classmethod
    def _require_nonempty_ids(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("pruned_ids must contain at least one ID")
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
