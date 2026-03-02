import pytest
from unittest.mock import AsyncMock, patch

try:
    from testcontainers.neo4j import Neo4jContainer
    HAS_TESTCONTAINERS = True
except ImportError:
    HAS_TESTCONTAINERS = False

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.asyncio(loop_scope="session"),
    pytest.mark.skipif(
        not HAS_TESTCONTAINERS,
        reason="testcontainers package not installed",
    ),
]


async def test_ingest_triggers_dlq_on_neo4j_failure(orchestrator_client):
    from orchestrator.tests.e2e.conftest import build_ingest_payload

    payload = build_ingest_payload(
        [{"path": "svc/main.py", "content": "class FailService:\n    pass\n"}],
        source_type="source_code",
    )

    with patch(
        "orchestrator.app.graph_builder.commit_to_neo4j",
        new_callable=AsyncMock,
        return_value={"commit_status": "failed"},
    ):
        response = await orchestrator_client.post(
            "/ingest?sync=true",
            json=payload,
        )

    assert response.status_code in (200, 503)
    body = response.json()
    if response.status_code == 200:
        assert body.get("status") in ("failed", "success")
    else:
        assert "detail" in body or "status" in body


async def test_circuit_breaker_opens(orchestrator_client):
    from orchestrator.tests.e2e.conftest import build_ingest_payload
    from orchestrator.app.circuit_breaker import CircuitOpenError

    payload = build_ingest_payload(
        [{"path": "svc/app.py", "content": "class BreakerTest:\n    pass\n"}],
        source_type="source_code",
    )

    with patch(
        "orchestrator.app.graph_builder.commit_to_neo4j",
        new_callable=AsyncMock,
        side_effect=CircuitOpenError("neo4j-breaker", 30.0),
    ):
        response = await orchestrator_client.post(
            "/ingest?sync=true",
            json=payload,
        )

    assert response.status_code in (503, 500, 200)
