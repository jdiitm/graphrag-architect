from __future__ import annotations

import pytest

from orchestrator.app.tenant_security import (
    build_traversal_batched_neighbor,
    build_traversal_neighbor_discovery,
)
from orchestrator.app.agentic_traversal import _NEIGHBOR_DISCOVERY_NO_ACL


class TestNeighborDiscoveryPagerankOrdering:
    def test_acl_variant_orders_by_pagerank(self) -> None:
        cypher = build_traversal_neighbor_discovery()
        assert "ORDER BY" in cypher
        assert "pagerank" in cypher.lower()

    def test_no_acl_variant_orders_by_pagerank(self) -> None:
        assert "ORDER BY" in _NEIGHBOR_DISCOVERY_NO_ACL
        assert "pagerank" in _NEIGHBOR_DISCOVERY_NO_ACL.lower()

    def test_ordering_before_limit(self) -> None:
        cypher = build_traversal_neighbor_discovery()
        order_pos = cypher.upper().index("ORDER BY")
        limit_pos = cypher.upper().index("LIMIT")
        assert order_pos < limit_pos


class TestBatchedNeighborPagerankOrdering:
    def test_acl_variant_orders_by_pagerank(self) -> None:
        cypher = build_traversal_batched_neighbor()
        assert "ORDER BY" in cypher
        assert "pagerank" in cypher.lower()

    def test_ordering_before_limit(self) -> None:
        cypher = build_traversal_batched_neighbor()
        order_pos = cypher.upper().index("ORDER BY")
        limit_pos = cypher.upper().index("LIMIT")
        assert order_pos < limit_pos


class TestPagerankFallbackGraceful:
    def test_uses_coalesce_for_missing_pagerank(self) -> None:
        cypher = build_traversal_neighbor_discovery()
        assert "coalesce" in cypher.lower()

    def test_batched_uses_coalesce(self) -> None:
        cypher = build_traversal_batched_neighbor()
        assert "coalesce" in cypher.lower()
