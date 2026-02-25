from __future__ import annotations

import re
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.cypher_sandbox import SandboxedQueryExecutor, CypherSandboxConfig
from orchestrator.app.cypher_tokenizer import TokenType, tokenize_cypher


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


class TestNoExplainPreCheck:

    @pytest.mark.asyncio
    async def test_execute_read_does_not_run_explain(self) -> None:
        config = CypherSandboxConfig()
        executor = SandboxedQueryExecutor(config)

        queries_run: list[str] = []
        mock_session = AsyncMock()

        async def _tx(tx):
            return [{"n": "result"}]

        mock_session.execute_read = _tx

        original_run = mock_session.run

        async def _tracking_run(query, **kwargs):
            queries_run.append(query)
            return original_run(query, **kwargs)

        mock_session.run = _tracking_run

        await executor.execute_read(
            mock_session, "MATCH (n:Service) RETURN n", {},
        )

        explain_queries = [q for q in queries_run if "EXPLAIN" in q.upper()]
        assert len(explain_queries) == 0, (
            "execute_read must NOT run EXPLAIN pre-checks. "
            "Use Neo4j transaction timeout instead. "
            f"Found EXPLAIN queries: {explain_queries}"
        )

    def test_config_has_query_timeout(self) -> None:
        config = CypherSandboxConfig(query_timeout_seconds=30.0)
        assert config.query_timeout_seconds == 30.0


class TestTimeoutEnforcement:

    def test_config_has_query_timeout(self) -> None:
        config = CypherSandboxConfig(query_timeout_seconds=30.0)
        assert config.query_timeout_seconds == 30.0

    def test_default_timeout(self) -> None:
        config = CypherSandboxConfig()
        assert config.query_timeout_seconds > 0


class TestCommentBypassDefeatTests:

    def test_line_comment_limit_bypass(self) -> None:
        config = CypherSandboxConfig(max_results=1000)
        executor = SandboxedQueryExecutor(config)
        cypher = "MATCH (n) RETURN n // LIMIT 10"
        result = executor.inject_limit(cypher)
        effective_limits = re.findall(
            r"(?<!//)(?<!/\*)\bLIMIT\s+(\d+)", result, re.IGNORECASE,
        )
        assert len(effective_limits) >= 1, (
            "A commented-out LIMIT must not suppress real limit injection. "
            f"Got: {result}"
        )
        for val in effective_limits:
            assert int(val) <= 1000

    def test_block_comment_limit_bypass(self) -> None:
        config = CypherSandboxConfig(max_results=500)
        executor = SandboxedQueryExecutor(config)
        cypher = "MATCH (n) RETURN n /* LIMIT 10 */"
        result = executor.inject_limit(cypher)
        tokens = tokenize_cypher(result)
        real_limits = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD
            and t.value.upper() == "LIMIT"
        ]
        assert len(real_limits) >= 1, (
            "A block-commented LIMIT must not suppress real limit injection. "
            f"Got: {result}"
        )

    def test_real_limit_preserved_alongside_line_comment(self) -> None:
        config = CypherSandboxConfig(max_results=1000)
        executor = SandboxedQueryExecutor(config)
        cypher = "MATCH (n) RETURN n LIMIT 5 // some comment"
        result = executor.inject_limit(cypher)
        tokens = tokenize_cypher(result)
        real_limits = [
            t for t in tokens
            if t.token_type == TokenType.KEYWORD
            and t.value.upper() == "LIMIT"
        ]
        assert len(real_limits) >= 1, (
            "Real LIMIT must be preserved when a line comment follows. "
            f"Got: {result}"
        )

    def test_only_comment_limit_injects_real_limit(self) -> None:
        config = CypherSandboxConfig(max_results=200)
        executor = SandboxedQueryExecutor(config)
        cypher = "MATCH (n) RETURN n // LIMIT 999999"
        result = executor.inject_limit(cypher)
        tokens = tokenize_cypher(result)
        non_comment_tokens = [
            t for t in tokens if t.token_type != TokenType.COMMENT
        ]
        reconstructed = "".join(t.value for t in non_comment_tokens)
        effective_limits = re.findall(
            r"\bLIMIT\s+(\d+)", reconstructed, re.IGNORECASE,
        )
        assert len(effective_limits) >= 1, (
            "When only a commented LIMIT exists, a real LIMIT must be injected. "
            f"Got: {result}"
        )
        for val in effective_limits:
            assert int(val) <= 200, (
                f"Injected LIMIT must respect max_results (200), got {val}"
            )

    def test_mixed_block_comment_and_real_limit(self) -> None:
        config = CypherSandboxConfig(max_results=100)
        executor = SandboxedQueryExecutor(config)
        cypher = "MATCH (n) /* LIMIT 99999 */ RETURN n LIMIT 50"
        result = executor.inject_limit(cypher)
        tokens = tokenize_cypher(result)
        non_comment_tokens = [
            t for t in tokens if t.token_type != TokenType.COMMENT
        ]
        reconstructed = "".join(t.value for t in non_comment_tokens)
        effective_limits = re.findall(
            r"\bLIMIT\s+(\d+)", reconstructed, re.IGNORECASE,
        )
        assert len(effective_limits) >= 1
        for val in effective_limits:
            assert int(val) <= 100


class TestUnwindAmplificationRejection:

    def test_detect_unwind_amplification_positive(self) -> None:
        from orchestrator.app.cypher_sandbox import detect_unwind_amplification

        cypher = "MATCH (n) WITH n LIMIT 10 UNWIND range(1, 1000000) AS x RETURN n, x"
        assert detect_unwind_amplification(cypher) is True

    def test_detect_unwind_amplification_negative(self) -> None:
        from orchestrator.app.cypher_sandbox import detect_unwind_amplification

        cypher = "MATCH (n:Service) RETURN n LIMIT 10"
        assert detect_unwind_amplification(cypher) is False

    @pytest.mark.asyncio
    async def test_execute_read_rejects_unwind_amplification(self) -> None:
        from orchestrator.app.cypher_sandbox import CypherAmplificationError

        config = CypherSandboxConfig()
        executor = SandboxedQueryExecutor(config)
        session = AsyncMock()

        cypher = "MATCH (n) WITH n LIMIT 10 UNWIND range(1, 1000000) AS x RETURN n, x"
        with pytest.raises(CypherAmplificationError):
            await executor.execute_read(session, cypher)

    @pytest.mark.asyncio
    async def test_execute_read_allows_safe_queries(self) -> None:
        config = CypherSandboxConfig()
        executor = SandboxedQueryExecutor(config)

        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[{"n": "result"}])

        mock_tx = AsyncMock()
        mock_tx.run = AsyncMock(return_value=mock_result)

        mock_session = AsyncMock()

        async def _execute_read(tx_fn, **kwargs):
            return await tx_fn(mock_tx)

        mock_session.execute_read = _execute_read

        cypher = "MATCH (n:Service) RETURN n"
        result = await executor.execute_read(mock_session, cypher)
        assert result == [{"n": "result"}]


class TestSandboxConfig:

    def test_defaults(self) -> None:
        config = CypherSandboxConfig()
        assert config.max_results == 1000
        assert config.query_timeout_seconds == 30.0
