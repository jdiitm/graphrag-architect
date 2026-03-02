from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Protocol

from pydantic import ValidationError

from orchestrator.app.mutation_publisher import GraphMutationEvent

logger = logging.getLogger(__name__)


class ConsumerTransport(Protocol):
    events: List[Dict[str, Any]]


class VectorDeleter(Protocol):
    async def delete(self, ids: List[str]) -> None: ...


class VectorUpserter(Protocol):
    async def upsert(
        self,
        entity_ids: List[str],
        tenant_id: str,
    ) -> None: ...


_DELETE_MUTATIONS = frozenset({"edge_tombstone", "node_delete"})
_UPSERT_MUTATIONS = frozenset({"node_upsert", "edge_upsert"})


class VectorSyncKafkaConsumer:
    def __init__(
        self,
        transport: ConsumerTransport,
        vector_deleter: VectorDeleter,
        vector_upserter: Optional[VectorUpserter] = None,
    ) -> None:
        self._transport = transport
        self._deleter = vector_deleter
        self._upserter = vector_upserter
        self._processed_count = 0

    @property
    def processed_count(self) -> int:
        return self._processed_count

    async def process_event(self, event: GraphMutationEvent) -> None:
        if event.mutation_type in _DELETE_MUTATIONS:
            await self._deleter.delete(event.entity_ids)
        elif event.mutation_type in _UPSERT_MUTATIONS:
            if self._upserter is not None:
                await self._upserter.upsert(
                    entity_ids=event.entity_ids,
                    tenant_id=event.tenant_id,
                )
            else:
                logger.warning(
                    "Received %s event but no upserter configured; "
                    "%d entity IDs dropped",
                    event.mutation_type,
                    len(event.entity_ids),
                )
        self._processed_count += 1

    async def process_raw(self, payload: Dict[str, Any]) -> None:
        try:
            event = GraphMutationEvent(**payload)
        except (ValidationError, TypeError) as exc:
            logger.warning("Malformed mutation event, skipping: %s", exc)
            return
        await self.process_event(event)
