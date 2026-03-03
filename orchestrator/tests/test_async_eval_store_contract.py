from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_background_evaluation_awaits_async_eval_store_put() -> None:
    from orchestrator.app.query_engine import _run_background_evaluation

    class _AsyncStore:
        def __init__(self) -> None:
            self.saved = {}

        async def put(self, query_id: str, payload: dict[str, object]) -> None:
            self.saved[query_id] = dict(payload)

    store = _AsyncStore()
    state = {"query": "which service owns auth?", "tenant_id": "t1"}
    with (
        patch("orchestrator.app.query_engine._EVAL_STORE", store),
        patch("orchestrator.app.query_engine._embed_query", new_callable=AsyncMock, return_value=None),
    ):
        await _run_background_evaluation("q-async", state)

    assert store.saved["q-async"]["retrieval_quality"] == "no_embedding"


@pytest.mark.asyncio
async def test_result_to_response_awaits_async_eval_store_get() -> None:
    from orchestrator.app.main import _result_to_response

    async_store = SimpleNamespace(
        get=AsyncMock(return_value={
            "query_id": "q-async",
            "evaluation_score": 0.88,
            "retrieval_quality": "high",
        })
    )
    with patch("orchestrator.app.main._EVAL_STORE", async_store):
        response = await _result_to_response({
            "answer": "ok",
            "sources": [],
            "complexity": "entity_lookup",
            "retrieval_path": "vector",
            "query_id": "q-async",
            "evaluation_score": None,
            "retrieval_quality": "pending",
        })

    assert response.evaluation_score == 0.88
    assert response.retrieval_quality == "high"
