from __future__ import annotations

from typing import Any, Dict, Protocol, runtime_checkable

IngestionState = Dict[str, Any]


@runtime_checkable
class IngestionStage(Protocol):
    async def execute(self, state: IngestionState) -> IngestionState: ...
    async def healthcheck(self) -> bool: ...
