from __future__ import annotations

from orchestrator.app.tenant_security import (
    build_traversal_batched_neighbor,
    build_traversal_batched_supernode_neighbor,
    build_traversal_neighbor_discovery,
    build_traversal_sampled_neighbor,
)
from orchestrator.app.agentic_traversal import (
    _SAMPLED_NEIGHBOR_TEMPLATE,
    _NEIGHBOR_DISCOVERY_TEMPLATE,
    _BATCHED_NEIGHBOR_TEMPLATE,
)


class TestPagerankPrefilterInTemplates:

    def test_neighbor_discovery_has_pagerank_threshold(self) -> None:
        query = build_traversal_neighbor_discovery()
        assert "$min_pagerank" in query, (
            "Neighbor discovery template must pre-filter with "
            "WHERE target.pagerank >= $min_pagerank to leverage indexes"
        )

    def test_sampled_neighbor_has_pagerank_threshold(self) -> None:
        query = build_traversal_sampled_neighbor()
        assert "$min_pagerank" in query, (
            "Sampled neighbor template must pre-filter with "
            "WHERE target.pagerank >= $min_pagerank"
        )

    def test_batched_neighbor_has_pagerank_threshold(self) -> None:
        query = build_traversal_batched_neighbor()
        assert "$min_pagerank" in query, (
            "Batched neighbor template must pre-filter with "
            "WHERE ... pagerank >= $min_pagerank"
        )

    def test_batched_supernode_neighbor_has_pagerank_threshold(self) -> None:
        query = build_traversal_batched_supernode_neighbor()
        assert "$min_pagerank" in query, (
            "Batched supernode template must pre-filter with "
            "WHERE ... pagerank >= $min_pagerank"
        )


class TestAgenticTraversalPagerankPrefilter:

    def test_sampled_template_has_min_pagerank_param(self) -> None:
        assert "$min_pagerank" in _SAMPLED_NEIGHBOR_TEMPLATE

    def test_neighbor_discovery_template_has_min_pagerank_param(self) -> None:
        assert "$min_pagerank" in _NEIGHBOR_DISCOVERY_TEMPLATE

    def test_batched_template_has_min_pagerank_param(self) -> None:
        assert "$min_pagerank" in _BATCHED_NEIGHBOR_TEMPLATE


class TestSchemaIndexForCentrality:

    def test_schema_contains_pagerank_composite_index(self) -> None:
        with open("orchestrator/app/schema_init.cypher") as fh:
            schema = fh.read()
        assert "pagerank" in schema, (
            "schema_init.cypher must define a composite index on "
            "(tenant_id, pagerank) for index-backed traversal ordering"
        )
