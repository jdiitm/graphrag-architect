from __future__ import annotations

import logging
from typing import Any, Optional

from orchestrator.app.stages import IngestionState

logger = logging.getLogger(__name__)


class VectorSyncStage:
    def __init__(
        self,
        outbox: Any,
        kafka_publisher: Optional[Any] = None,
        backend: str = "memory",
    ) -> None:
        self._outbox = outbox
        self._kafka_publisher = kafka_publisher
        self._backend = backend

    async def execute(self, state: IngestionState) -> IngestionState:
        events = state.get("mutation_events", [])
        if not events:
            state["vector_sync_status"] = "skipped"
            return state

        if self._backend == "kafka" and self._kafka_publisher is not None:
            for event in events:
                await self._kafka_publisher.publish(event)
            state["vector_sync_status"] = "published"
            return state

        for event in events:
            self._outbox.enqueue(event)
        state["vector_sync_status"] = "enqueued"
        return state

    async def healthcheck(self) -> bool:
        if self._backend == "kafka" and self._kafka_publisher is not None:
            return await self._kafka_publisher.healthcheck()
        return True
