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


class TestNestedSubqueryLimitEnforcement:

    def test_inner_limit_in_subquery_is_capped(self) -> None:
        config = CypherSandboxConfig(max_results=100)
        executor = SandboxedQueryExecutor(config)
        cypher = (
            "MATCH (n:Service) "
            "WHERE n.id IN [x IN "
            "[(m) IN MATCH (m:Service) RETURN m LIMIT 9999] "
            "| x.id] "
            "RETURN n LIMIT 100"
        )
        result = executor.inject_limit(cypher)
        assert "9999" not in result, (
            "Nested LIMIT 9999 must be capped by the sandbox"
        )

    def test_call_subquery_limit_is_capped(self) -> None:
        config = CypherSandboxConfig(max_results=100)
        executor = SandboxedQueryExecutor(config)
        cypher = (
            "MATCH (n:Service) "
            "CALL { MATCH (m:Database) RETURN m LIMIT 50000 } "
            "RETURN n, m LIMIT 100"
        )
        result = executor.inject_limit(cypher)
        assert "50000" not in result, (
            "CALL subquery LIMIT 50000 must be capped to max_results"
        )

    def test_all_limit_clauses_are_enforced(self) -> None:
        config = CypherSandboxConfig(max_results=200)
        executor = SandboxedQueryExecutor(config)
        cypher = "MATCH (a) RETURN a LIMIT 5000 UNION MATCH (b) RETURN b LIMIT 5000"
        result = executor.inject_limit(cypher)
        limits = re.findall(r"LIMIT\s+(\d+)", result, re.IGNORECASE)
        for limit_val in limits:
            assert int(limit_val) <= 200, (
                f"All LIMIT values must be <= max_results (200), "
                f"found LIMIT {limit_val}"
            )

    def test_bypass_small_outer_large_inner_limit(self) -> None:
        config = CypherSandboxConfig(max_results=100)
        executor = SandboxedQueryExecutor(config)
        cypher = (
            "MATCH (n:Service) RETURN n LIMIT 50 "
            "UNION "
            "MATCH (m:Service) RETURN m LIMIT 9999"
        )
        result = executor.inject_limit(cypher)
        limits = re.findall(r"LIMIT\s+(\d+)", result, re.IGNORECASE)
        for limit_val in limits:
            assert int(limit_val) <= 100, (
                f"When first LIMIT is within bounds but a subsequent "
                f"LIMIT exceeds max_results, ALL limits must still be "
                f"capped. Found LIMIT {limit_val}"
            )


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
