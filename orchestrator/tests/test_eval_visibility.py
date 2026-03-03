from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_result_to_response_enriches_with_stored_evaluation() -> None:
    from orchestrator.app.main import _result_to_response

    store = MagicMock()
    store.get.return_value = {
        "query_id": "q-1",
        "evaluation_score": 0.91,
        "retrieval_quality": "good",
        "judge_model": "llm",
    }
    with patch("orchestrator.app.main._EVAL_STORE", store):
        response = await _result_to_response({
            "answer": "ok",
            "sources": [],
            "complexity": "entity_lookup",
            "retrieval_path": "vector",
            "query_id": "q-1",
            "evaluation_score": None,
            "retrieval_quality": "pending",
        })

    assert response.evaluation_score == 0.91
    assert response.retrieval_quality == "good"
    assert response.evaluation_details is not None
    assert response.evaluation_details["judge_model"] == "llm"


@pytest.mark.asyncio
async def test_result_to_response_keeps_inline_evaluation_when_store_empty() -> None:
    from orchestrator.app.main import _result_to_response

    store = MagicMock()
    store.get.return_value = None
    with patch("orchestrator.app.main._EVAL_STORE", store):
        response = await _result_to_response({
            "answer": "ok",
            "sources": [],
            "complexity": "entity_lookup",
            "retrieval_path": "vector",
            "query_id": "q-2",
            "evaluation_score": 0.5,
            "retrieval_quality": "pending",
        })

    assert response.evaluation_score == 0.5
    assert response.retrieval_quality == "pending"
    assert response.evaluation_details is None
