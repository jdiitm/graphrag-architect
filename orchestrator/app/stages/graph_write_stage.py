from __future__ import annotations

import logging
from typing import Any

from orchestrator.app.stages import IngestionState

logger = logging.getLogger(__name__)


class GraphWriteStage:
    def __init__(self, repository: Any) -> None:
        self._repository = repository

    async def execute(self, state: IngestionState) -> IngestionState:
        entities = state.get("extracted_nodes", [])
        if not entities:
            state["commit_status"] = "skipped"
            return state

        try:
            await self._repository.commit_topology(entities)
            state["commit_status"] = "success"
        except Exception as exc:
            logger.error("Graph write failed: %s", exc)
            state["commit_status"] = "failed"

        return state

    async def healthcheck(self) -> bool:
        try:
            await self._repository.read_topology()
            return True
        except Exception:
            return False
