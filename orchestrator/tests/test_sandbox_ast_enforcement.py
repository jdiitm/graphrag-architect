import pytest
from unittest.mock import AsyncMock, MagicMock

from orchestrator.app.cypher_sandbox import (
    CypherSandboxConfig,
    SandboxedQueryExecutor,
)
from orchestrator.app.cypher_validator import CypherValidationError


@pytest.fixture
def sandbox():
    return SandboxedQueryExecutor(
        config=CypherSandboxConfig(max_results=100, max_query_cost=10),
    )


@pytest.fixture
def mock_session():
    session = AsyncMock()
    session.execute_read = AsyncMock(return_value=[{"n": "ok"}])
    return session


class TestWriteOperationBlocking:
    def test_merge_rejected(self, sandbox, mock_session):
        loop = __import__("asyncio").new_event_loop()
        with pytest.raises(CypherValidationError, match="write operation"):
            loop.run_until_complete(
                sandbox.execute_read(mock_session, "MERGE (n:Node {id: '1'})")
            )
        loop.close()

    def test_create_rejected(self, sandbox, mock_session):
        loop = __import__("asyncio").new_event_loop()
        with pytest.raises(CypherValidationError, match="write operation"):
            loop.run_until_complete(
                sandbox.execute_read(mock_session, "CREATE (n:Node {id: '1'})")
            )
        loop.close()

    def test_delete_rejected(self, sandbox, mock_session):
        loop = __import__("asyncio").new_event_loop()
        with pytest.raises(CypherValidationError, match="write operation"):
            loop.run_until_complete(
                sandbox.execute_read(mock_session, "MATCH (n) DELETE n")
            )
        loop.close()


class TestUnboundedPathBlocking:
    def test_unbounded_star_rejected(self, sandbox, mock_session):
        loop = __import__("asyncio").new_event_loop()
        with pytest.raises(CypherValidationError, match="Unbounded"):
            loop.run_until_complete(
                sandbox.execute_read(
                    mock_session,
                    "MATCH (a)-[*]->(b) RETURN b",
                )
            )
        loop.close()


class TestQueryCostGate:
    def test_high_cost_query_rejected(self, sandbox, mock_session):
        loop = __import__("asyncio").new_event_loop()
        cypher = (
            "MATCH (a)-[*1..5]->(b) "
            "MATCH (b)-[*1..5]->(c) "
            "MATCH (c)-[*1..5]->(d) "
            "RETURN d"
        )
        with pytest.raises(CypherValidationError):
            loop.run_until_complete(
                sandbox.execute_read(mock_session, cypher)
            )
        loop.close()


class TestCartesianProductBlocking:
    def test_cartesian_product_rejected(self, sandbox, mock_session):
        loop = __import__("asyncio").new_event_loop()
        with pytest.raises(CypherValidationError, match="Cartesian"):
            loop.run_until_complete(
                sandbox.execute_read(
                    mock_session,
                    "MATCH (a:Service), (b:Topic) RETURN a, b",
                )
            )
        loop.close()


class TestCallSubqueryBlocking:
    def test_call_subquery_rejected(self, sandbox, mock_session):
        loop = __import__("asyncio").new_event_loop()
        with pytest.raises(CypherValidationError, match="CALL subquery"):
            loop.run_until_complete(
                sandbox.execute_read(
                    mock_session,
                    "CALL { MATCH (n) RETURN n } RETURN n",
                )
            )
        loop.close()
