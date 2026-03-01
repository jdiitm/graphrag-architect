from __future__ import annotations

import logging
from typing import Any, List

from orchestrator.app.stages import IngestionState

logger = logging.getLogger(__name__)


class ExtractionStage:
    def __init__(self, extractor: Any) -> None:
        self._extractor = extractor

    async def execute(self, state: IngestionState) -> IngestionState:
        raw_files = state.get("raw_files", [])
        if not raw_files:
            return state

        existing_nodes: List[Any] = list(state.get("extracted_nodes", []))
        result = await self._extractor.extract_all(raw_files)

        for svc in result.services:
            svc.confidence = 0.7
            existing_nodes.append(svc)

        for call in result.calls:
            call.confidence = 0.7
            existing_nodes.append(call)

        state["extracted_nodes"] = existing_nodes
        return state

    async def healthcheck(self) -> bool:
        return True
