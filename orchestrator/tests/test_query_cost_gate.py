from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.cypher_validator import (
    CypherValidationError,
    estimate_query_cost,
)


class TestEstimateQueryCostBasics:

    def test_simple_match_has_low_cost(self) -> None:
        cost = estimate_query_cost("MATCH (n:Service) RETURN n")
        assert cost <= 3

    def test_multiple_match_increases_cost(self) -> None:
        cypher = (
            "MATCH (a:Service) "
            "MATCH (b:Service) "
            "MATCH (c:Service) "
            "RETURN a, b, c"
        )
        cost = estimate_query_cost(cypher)
        assert cost >= 4

    def test_variable_length_path_increases_cost(self) -> None:
        cypher = "MATCH (a:Service)-[:CALLS*1..3]->(b:Service) RETURN a, b"
        cost = estimate_query_cost(cypher)
        assert cost >= 7

    def test_exceeding_max_path_depth_raises(self) -> None:
        cypher = "MATCH (a)-[:CALLS*1..10]->(b) RETURN a, b"
        with pytest.raises(CypherValidationError, match="exceeds maximum"):
            estimate_query_cost(cypher)

    def test_unbounded_path_raises(self) -> None:
        cypher = "MATCH (a)-[:CALLS*]->(b) RETURN a"
        with pytest.raises(CypherValidationError, match="Unbounded"):
            estimate_query_cost(cypher)


class TestQueryCostGateInQueryEngine:

    @pytest.mark.asyncio
    async def test_expensive_query_rejected_before_execution(self) -> None:
        from orchestrator.app.query_engine import _execute_sandboxed_read

        expensive_cypher = (
            "MATCH (a:Service)-[:CALLS*1..5]->(b:Service) "
            "MATCH (c:Service)-[:CALLS]->(d:Service) "
            "MATCH (e:Service) "
            "RETURN a, b, c, d, e"
        )
        mock_driver = AsyncMock()

        with patch.dict(os.environ, {"MAX_QUERY_COST": "5"}):
            with pytest.raises(CypherValidationError, match="cost"):
                await _execute_sandboxed_read(
                    mock_driver, expensive_cypher, {},
                )

        mock_driver.session.assert_not_called()

    @pytest.mark.asyncio
    async def test_cheap_query_proceeds_to_execution(self) -> None:
        from orchestrator.app.query_engine import _execute_sandboxed_read

        cheap_cypher = "MATCH (n:Service) RETURN n LIMIT 10"
        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(return_value=[{"name": "svc"}])
        mock_driver = MagicMock()
        mock_driver.session.return_value = AsyncMock(
            __aenter__=AsyncMock(return_value=mock_session),
            __aexit__=AsyncMock(return_value=False),
        )

        with patch.dict(os.environ, {"MAX_QUERY_COST": "50"}):
            result = await _execute_sandboxed_read(
                mock_driver, cheap_cypher, {},
            )

        assert result is not None
        mock_session.execute_read.assert_called_once()

    @pytest.mark.asyncio
    async def test_default_max_cost_allows_reasonable_queries(self) -> None:
        from orchestrator.app.query_engine import _execute_sandboxed_read

        reasonable_cypher = "MATCH (a:Service)-[:CALLS]->(b:Service) RETURN a, b"
        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(return_value=[])
        mock_driver = MagicMock()
        mock_driver.session.return_value = AsyncMock(
            __aenter__=AsyncMock(return_value=mock_session),
            __aexit__=AsyncMock(return_value=False),
        )

        env_without_max = {k: v for k, v in os.environ.items() if k != "MAX_QUERY_COST"}
        with patch.dict(os.environ, env_without_max, clear=True):
            result = await _execute_sandboxed_read(
                mock_driver, reasonable_cypher, {},
            )

        assert result is not None or result == []


class TestQueryCostConfigurable:

    def test_max_query_cost_from_env(self) -> None:
        from orchestrator.app.query_engine import _get_max_query_cost

        with patch.dict(os.environ, {"MAX_QUERY_COST": "42"}):
            assert _get_max_query_cost() == 42

    def test_max_query_cost_default(self) -> None:
        from orchestrator.app.query_engine import _get_max_query_cost

        env_without = {k: v for k, v in os.environ.items() if k != "MAX_QUERY_COST"}
        with patch.dict(os.environ, env_without, clear=True):
            assert _get_max_query_cost() == 20
