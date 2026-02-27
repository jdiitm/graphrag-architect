from __future__ import annotations

import pytest

from orchestrator.app.agentic_traversal import _SAMPLED_NEIGHBOR_TEMPLATE


class TestSampledNeighborTemplateDeterminism:

    def test_template_orders_by_pagerank_first(self) -> None:
        order_section = _SAMPLED_NEIGHBOR_TEMPLATE[
            _SAMPLED_NEIGHBOR_TEMPLATE.index("ORDER BY"):
        ]
        assert "pagerank" in order_section
        pr_pos = order_section.index("pagerank")
        id_pos = order_section.index("target.id")
        assert pr_pos < id_pos

    def test_template_orders_by_degree_second(self) -> None:
        order_section = _SAMPLED_NEIGHBOR_TEMPLATE[
            _SAMPLED_NEIGHBOR_TEMPLATE.index("ORDER BY"):
        ]
        assert "degree" in order_section
        degree_pos = order_section.index("degree")
        id_pos = order_section.index("target.id")
        assert degree_pos < id_pos

    def test_template_uses_coalesce_for_pagerank(self) -> None:
        assert "coalesce(target.pagerank, 0)" in _SAMPLED_NEIGHBOR_TEMPLATE

    def test_template_uses_descending_order(self) -> None:
        order_section = _SAMPLED_NEIGHBOR_TEMPLATE[
            _SAMPLED_NEIGHBOR_TEMPLATE.index("ORDER BY"):
        ]
        assert "DESC" in order_section

    def test_template_uses_deterministic_tiebreaker(self) -> None:
        assert "target.id" in _SAMPLED_NEIGHBOR_TEMPLATE
        assert "rand()" not in _SAMPLED_NEIGHBOR_TEMPLATE

    def test_template_preserves_limit_parameter(self) -> None:
        assert "LIMIT $sample_size" in _SAMPLED_NEIGHBOR_TEMPLATE

    def test_template_preserves_tenant_and_acl_filters(self) -> None:
        assert "tenant_id" in _SAMPLED_NEIGHBOR_TEMPLATE
        assert "tombstoned_at IS NULL" in _SAMPLED_NEIGHBOR_TEMPLATE
        assert "acl_team" in _SAMPLED_NEIGHBOR_TEMPLATE
