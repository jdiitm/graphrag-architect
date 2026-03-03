from __future__ import annotations

import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.tenant_isolation import StructuredTenantAuditLogger


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record.getMessage())


@pytest.mark.asyncio
async def test_synthesize_answer_emits_complete_audit_log() -> None:
    from orchestrator.app.query_engine import synthesize_answer

    state = {
        "query": "what breaks if auth fails?",
        "tenant_id": "t1",
        "complexity": "multi_hop",
        "cypher_query": "MATCH (n)-[r]->(m) RETURN n,m",
        "candidates": [{"id": "auth", "name": "auth"}],
        "cypher_results": [{"source": "auth", "target": "orders"}],
        "authorization": "",
    }
    mock_audit = MagicMock()

    with (
        patch(
            "orchestrator.app.query_engine._SEC_AUDIT",
            mock_audit,
        ),
        patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="auth impacts orders",
        ),
    ):
        result = await synthesize_answer(state)

    assert result["answer"] == "auth impacts orders"
    assert mock_audit.log_query.call_count == 1
    kwargs = mock_audit.log_query.call_args.kwargs
    assert kwargs["tenant_id"] == "t1"
    assert kwargs["raw_user_query"] == "what breaks if auth fails?"
    assert kwargs["cypher_query"] == "MATCH (n)-[r]->(m) RETURN n,m"
    assert "auth" in kwargs["node_ids_returned"]


def test_structured_tenant_logger_includes_query_and_nodes() -> None:
    handler = _CaptureHandler()
    log = logging.getLogger("test.tenant.audit")
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    logger = StructuredTenantAuditLogger()
    logger._logger = log

    logger.log_query(
        tenant_id="t1",
        query_hash="abc",
        result_count=2,
        raw_user_query="q",
        cypher_query="MATCH ...",
        node_ids_returned=["a", "b"],
    )
    parsed = json.loads(handler.records[0])
    assert parsed["tenant_id"] == "t1"
    assert parsed["raw_user_query"] == "q"
    assert parsed["cypher_query"] == "MATCH ..."
    assert parsed["node_ids_returned"] == ["a", "b"]
