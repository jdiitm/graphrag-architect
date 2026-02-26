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

    @field_validator("pruned_ids")
    @classmethod
    def _require_nonempty_ids(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("pruned_ids must contain at least one ID")
        return v


@runtime_checkable
class VectorDeleter(Protocol):
    async def delete(self, collection: str, ids: List[str]) -> int: ...


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
