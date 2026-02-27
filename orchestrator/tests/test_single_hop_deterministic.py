from __future__ import annotations

import pytest

from orchestrator.app.query_engine import _build_single_hop_cypher


class TestSingleHopDeterministicOrdering:

    def test_cypher_contains_order_by_clause(self) -> None:
        cypher = _build_single_hop_cypher()
        assert "ORDER BY" in cypher, (
            f"Single-hop Cypher must include ORDER BY for deterministic "
            f"results, got: {cypher}"
        )

    def test_order_uses_degree_desc_with_name_tiebreaker(self) -> None:
        cypher = _build_single_hop_cypher()
        order_idx = cypher.index("ORDER BY")
        limit_idx = cypher.index("LIMIT")
        order_clause = cypher[order_idx:limit_idx].strip()
        assert "m.degree DESC" in order_clause, (
            f"ORDER BY must rank by m.degree DESC, got: {order_clause}"
        )
        assert "m.name" in order_clause, (
            f"ORDER BY must have m.name tiebreaker for determinism, "
            f"got: {order_clause}"
        )

    def test_order_by_does_not_use_size_pattern(self) -> None:
        cypher = _build_single_hop_cypher()
        assert "size((n)--())" not in cypher, (
            "Must NOT use size((n)--()) which causes supernode "
            "materialization"
        )

    def test_cypher_still_has_degree_cap_filter(self) -> None:
        cypher = _build_single_hop_cypher()
        assert "degree_cap" in cypher, (
            f"Cypher must retain degree_cap filter: {cypher}"
        )

    def test_cypher_retains_tombstone_filter(self) -> None:
        cypher = _build_single_hop_cypher()
        assert "tombstoned_at IS NULL" in cypher
