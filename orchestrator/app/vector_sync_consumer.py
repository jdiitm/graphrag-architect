from __future__ import annotations

import logging
from typing import Any, Dict, List, Protocol

from pydantic import ValidationError

from orchestrator.app.mutation_publisher import GraphMutationEvent

logger = logging.getLogger(__name__)


class ConsumerTransport(Protocol):
    events: List[Dict[str, Any]]


class VectorDeleter(Protocol):
    async def delete(self, ids: List[str]) -> None: ...


_DELETE_MUTATIONS = frozenset({"edge_tombstone", "node_delete"})


class VectorSyncKafkaConsumer:
    def __init__(
        self,
        transport: Any,
        vector_deleter: Any,
    ) -> None:
        self._transport = transport
        self._deleter = vector_deleter
        self._processed_count = 0

    @property
    def processed_count(self) -> int:
        return self._processed_count

    async def process_event(self, event: GraphMutationEvent) -> None:
        if event.mutation_type in _DELETE_MUTATIONS:
            await self._deleter.delete(event.entity_ids)
        self._processed_count += 1

    async def process_raw(self, payload: Dict[str, Any]) -> None:
        try:
            event = GraphMutationEvent(**payload)
        except (ValidationError, TypeError) as exc:
            logger.warning("Malformed mutation event, skipping: %s", exc)
            return
        await self.process_event(event)
