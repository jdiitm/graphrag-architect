from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Protocol

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class GraphMutationEvent(BaseModel):
    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
    )
    mutation_type: Literal[
        "node_upsert", "edge_upsert", "edge_tombstone", "node_delete",
    ]
    entity_ids: List[str]
    tenant_id: str = ""
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
    )


class MutationTransport(Protocol):
    async def publish(
        self, topic: str, events: List[Dict[str, Any]],
    ) -> None: ...


class KafkaMutationPublisher:
    def __init__(
        self,
        transport: MutationTransport,
        topic: str = "graph.mutations",
    ) -> None:
        self._transport = transport
        self._topic = topic

    async def publish(self, events: List[GraphMutationEvent]) -> None:
        if not events:
            return
        payloads = [e.model_dump() for e in events]
        try:
            await self._transport.publish(self._topic, payloads)
        except Exception as exc:
            logger.warning(
                "Mutation publish failed for %d event(s): %s",
                len(events),
                exc,
            )
