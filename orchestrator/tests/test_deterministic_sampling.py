from __future__ import annotations

import pytest

from orchestrator.app.agentic_traversal import _SAMPLED_NEIGHBOR_TEMPLATE


class TestSampledNeighborTemplateDeterminism:

    def test_template_orders_by_pagerank_first(self) -> None:
        assert "pagerank" in _SAMPLED_NEIGHBOR_TEMPLATE
        pr_pos = _SAMPLED_NEIGHBOR_TEMPLATE.index("pagerank")
        rand_pos = _SAMPLED_NEIGHBOR_TEMPLATE.index("rand()")
        assert pr_pos < rand_pos

    def test_template_orders_by_degree_second(self) -> None:
        assert "degree" in _SAMPLED_NEIGHBOR_TEMPLATE
        degree_pos = _SAMPLED_NEIGHBOR_TEMPLATE.index("degree")
        rand_pos = _SAMPLED_NEIGHBOR_TEMPLATE.index("rand()")
        assert degree_pos < rand_pos

    def test_template_uses_coalesce_for_pagerank(self) -> None:
        assert "coalesce(target.pagerank, 0)" in _SAMPLED_NEIGHBOR_TEMPLATE

    def test_template_uses_descending_order(self) -> None:
        order_section = _SAMPLED_NEIGHBOR_TEMPLATE[
            _SAMPLED_NEIGHBOR_TEMPLATE.index("ORDER BY"):
        ]
        assert "DESC" in order_section

    def test_template_still_has_rand_as_tiebreaker(self) -> None:
        assert "rand()" in _SAMPLED_NEIGHBOR_TEMPLATE

    def test_template_preserves_limit_parameter(self) -> None:
        assert "LIMIT $sample_size" in _SAMPLED_NEIGHBOR_TEMPLATE

    def test_template_preserves_tenant_and_acl_filters(self) -> None:
        assert "tenant_id" in _SAMPLED_NEIGHBOR_TEMPLATE
        assert "tombstoned_at IS NULL" in _SAMPLED_NEIGHBOR_TEMPLATE
        assert "acl_team" in _SAMPLED_NEIGHBOR_TEMPLATE
