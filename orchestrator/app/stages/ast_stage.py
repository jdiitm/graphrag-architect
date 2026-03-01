from __future__ import annotations

import logging
from typing import Any, List, Optional

from orchestrator.app.stages import IngestionState

logger = logging.getLogger(__name__)


class ASTStage:
    def __init__(
        self,
        grpc_client: Optional[Any] = None,
    ) -> None:
        self._grpc_client = grpc_client

    async def execute(self, state: IngestionState) -> IngestionState:
        pending = state.get("pending_files", [])
        if not pending:
            return state

        if self._grpc_client is not None and self._grpc_client.is_available:
            file_pairs = [(f["path"], f["content"]) for f in pending]
            raw_results = await self._grpc_client.extract_batch(file_pairs)
            state["ast_results"] = raw_results
            return state

        results = self._local_extract(pending)
        state["ast_results"] = results
        return state

    async def healthcheck(self) -> bool:
        return True

    @staticmethod
    def _local_extract(
        pending: List[Any],
    ) -> List[Any]:
        from orchestrator.app.ast_extraction import GoASTExtractor, PythonASTExtractor
        go_extractor = GoASTExtractor()
        py_extractor = PythonASTExtractor()
        go_result = go_extractor.extract_all(pending)
        py_result = py_extractor.extract_all(pending)
        return [go_result, py_result]
