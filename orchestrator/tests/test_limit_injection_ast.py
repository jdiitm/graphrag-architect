from __future__ import annotations

import re

import pytest

from orchestrator.app.cypher_sandbox import SandboxedQueryExecutor, CypherSandboxConfig


class TestWithUnwindAmplificationDefeat:

    def test_with_unwind_amplification_is_detected(self) -> None:
        from orchestrator.app.cypher_sandbox import detect_unwind_amplification
        cypher = (
            "MATCH (n:Service) "
            "WITH n LIMIT 10 "
            "UNWIND range(1, 1000) AS x "
            "MATCH (m:Service) "
            "RETURN m"
        )
        assert detect_unwind_amplification(cypher) is True, (
            "Queries with UNWIND after WITH...LIMIT must be detected as "
            "potential amplification attacks"
        )

    def test_normal_unwind_is_not_flagged(self) -> None:
        from orchestrator.app.cypher_sandbox import detect_unwind_amplification
        cypher = (
            "UNWIND $batch AS row "
            "MATCH (n:Service {id: row.id}) "
            "RETURN n"
        )
        assert detect_unwind_amplification(cypher) is False, (
            "UNWIND at the start (not after WITH...LIMIT) must not be flagged"
        )

    def test_limit_injected_after_final_return(self) -> None:
        config = CypherSandboxConfig(max_results=500)
        executor = SandboxedQueryExecutor(config)
        cypher = "MATCH (n:Service)-[:CALLS]->(m:Service) RETURN n, m"
        result = executor.inject_limit(cypher)
        return_pos = result.upper().rfind("RETURN")
        limit_pos = result.upper().rfind("LIMIT")
        assert limit_pos > return_pos, (
            f"LIMIT must appear after the final RETURN. "
            f"RETURN at {return_pos}, LIMIT at {limit_pos}. "
            f"Result: {result}"
        )

    def test_existing_final_limit_is_preserved_and_capped(self) -> None:
        config = CypherSandboxConfig(max_results=100)
        executor = SandboxedQueryExecutor(config)
        cypher = "MATCH (n:Service) RETURN n LIMIT 50"
        result = executor.inject_limit(cypher)
        limits = re.findall(r"\bLIMIT\s+(\d+)", result, re.IGNORECASE)
        assert len(limits) >= 1
        for val in limits:
            assert int(val) <= 100

    def test_multiple_with_limits_all_capped(self) -> None:
        config = CypherSandboxConfig(max_results=200)
        executor = SandboxedQueryExecutor(config)
        cypher = (
            "MATCH (n:Service) "
            "WITH n LIMIT 5000 "
            "MATCH (n)-[:CALLS]->(m) "
            "RETURN m LIMIT 5000"
        )
        result = executor.inject_limit(cypher)
        limits = re.findall(r"\bLIMIT\s+(\d+)", result, re.IGNORECASE)
        for val in limits:
            assert int(val) <= 200, (
                f"All LIMIT values must be <= max_results (200), "
                f"found LIMIT {val}. Result: {result}"
            )
