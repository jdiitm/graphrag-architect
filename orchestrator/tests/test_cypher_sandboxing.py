from __future__ import annotations

import re
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.cypher_sandbox import SandboxedQueryExecutor, CypherSandboxConfig


class TestLimitInjection:

    def test_injects_limit_when_missing(self) -> None:
        config = CypherSandboxConfig(max_results=1000)
        executor = SandboxedQueryExecutor(config)
        cypher = "MATCH (n:Service) RETURN n"
        result = executor.inject_limit(cypher)
        assert "LIMIT" in result.upper()
        assert "1000" in result

    def test_preserves_existing_limit(self) -> None:
        config = CypherSandboxConfig(max_results=1000)
        executor = SandboxedQueryExecutor(config)
        cypher = "MATCH (n:Service) RETURN n LIMIT 50"
        result = executor.inject_limit(cypher)
        assert result.count("LIMIT") == 1
        assert "50" in result

    def test_caps_excessive_limit(self) -> None:
        config = CypherSandboxConfig(max_results=1000)
        executor = SandboxedQueryExecutor(config)
        cypher = "MATCH (n:Service) RETURN n LIMIT 999999"
        result = executor.inject_limit(cypher)
        assert "1000" in result


class TestExplainPreFlight:

    @pytest.mark.asyncio
    async def test_rejects_query_above_row_threshold(self) -> None:
        config = CypherSandboxConfig(max_estimated_rows=100_000)
        executor = SandboxedQueryExecutor(config)

        mock_session = AsyncMock()
        mock_result = AsyncMock()
        mock_result.data.return_value = [
            {"Plan": {"estimatedRows": 500_000}}
        ]

        async def _explain_run(query, **kwargs):
            return mock_result

        mock_session.run = _explain_run

        from orchestrator.app.cypher_sandbox import QueryTooExpensiveError

        with pytest.raises(QueryTooExpensiveError):
            await executor.explain_check(mock_session, "MATCH (n) RETURN n")

    @pytest.mark.asyncio
    async def test_allows_query_below_threshold(self) -> None:
        config = CypherSandboxConfig(max_estimated_rows=100_000)
        executor = SandboxedQueryExecutor(config)

        mock_session = AsyncMock()
        mock_result = AsyncMock()
        mock_result.data.return_value = [
            {"Plan": {"estimatedRows": 500}}
        ]

        async def _explain_run(query, **kwargs):
            return mock_result

        mock_session.run = _explain_run

        await executor.explain_check(mock_session, "MATCH (n) RETURN n LIMIT 10")


class TestTimeoutEnforcement:

    def test_config_has_query_timeout(self) -> None:
        config = CypherSandboxConfig(query_timeout_seconds=30.0)
        assert config.query_timeout_seconds == 30.0

    def test_default_timeout(self) -> None:
        config = CypherSandboxConfig()
        assert config.query_timeout_seconds > 0


class TestSandboxConfig:

    def test_defaults(self) -> None:
        config = CypherSandboxConfig()
        assert config.max_results == 1000
        assert config.max_estimated_rows == 100_000
        assert config.query_timeout_seconds == 30.0
