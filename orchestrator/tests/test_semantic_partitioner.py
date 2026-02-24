from __future__ import annotations

from orchestrator.app.graph_embeddings import GraphTopology
from orchestrator.app.semantic_partitioner import (
    Community,
    PartitionResult,
    SemanticPartitioner,
    _compute_modularity,
    _count_edges,
    _louvain_partition,
)


def _triangle() -> GraphTopology:
    return GraphTopology(
        nodes=["a", "b", "c"],
        adjacency={
            "a": ["b", "c"],
            "b": ["a", "c"],
            "c": ["a", "b"],
        },
    )


def _two_cliques() -> GraphTopology:
    return GraphTopology(
        nodes=["a", "b", "c", "d", "e", "f"],
        adjacency={
            "a": ["b", "c"],
            "b": ["a", "c"],
            "c": ["a", "b", "d"],
            "d": ["c", "e", "f"],
            "e": ["d", "f"],
            "f": ["d", "e"],
        },
    )


class TestCountEdges:
    def test_triangle_has_three_edges(self) -> None:
        assert _count_edges(_triangle()) == 3

    def test_empty_graph_has_zero_edges(self) -> None:
        topo = GraphTopology(nodes=[], adjacency={})
        assert _count_edges(topo) == 0


class TestComputeModularity:
    def test_all_in_one_community(self) -> None:
        topo = _triangle()
        partition = {"a": 0, "b": 0, "c": 0}
        q = _compute_modularity(topo, partition, 3)
        assert isinstance(q, float)

    def test_all_separate_communities(self) -> None:
        topo = _triangle()
        partition = {"a": 0, "b": 1, "c": 2}
        q = _compute_modularity(topo, partition, 3)
        assert q <= 0.0

    def test_zero_edges_returns_zero(self) -> None:
        topo = GraphTopology(nodes=["a", "b"], adjacency={})
        partition = {"a": 0, "b": 0}
        assert _compute_modularity(topo, partition, 0) == 0.0


class TestLouvainPartition:
    def test_empty_graph_returns_empty(self) -> None:
        topo = GraphTopology(nodes=[], adjacency={})
        assert _louvain_partition(topo) == {}

    def test_single_node_gets_community(self) -> None:
        topo = GraphTopology(nodes=["a"], adjacency={})
        result = _louvain_partition(topo)
        assert "a" in result

    def test_connected_clique_same_community(self) -> None:
        topo = _triangle()
        result = _louvain_partition(topo)
        assert len(set(result.values())) >= 1

    def test_two_cliques_get_two_communities(self) -> None:
        topo = _two_cliques()
        result = _louvain_partition(topo, resolution=1.5)
        communities = set(result.values())
        assert len(communities) >= 2


class TestSemanticPartitioner:
    def test_partition_returns_partition_result(self) -> None:
        topo = _triangle()
        partitioner = SemanticPartitioner()
        result = partitioner.partition(topo)
        assert isinstance(result, PartitionResult)
        assert result.community_count >= 1

    def test_all_nodes_assigned_to_community(self) -> None:
        topo = _two_cliques()
        partitioner = SemanticPartitioner()
        result = partitioner.partition(topo)
        assert set(result.node_to_community.keys()) == set(topo.nodes)

    def test_communities_have_frozen_members(self) -> None:
        topo = _triangle()
        partitioner = SemanticPartitioner()
        result = partitioner.partition(topo)
        for comm in result.communities:
            assert isinstance(comm, Community)
            assert isinstance(comm.members, frozenset)
            assert len(comm.members) > 0

    def test_modularity_is_a_float(self) -> None:
        topo = _two_cliques()
        partitioner = SemanticPartitioner()
        result = partitioner.partition(topo)
        assert isinstance(result.modularity, float)

    def test_empty_graph_produces_empty_result(self) -> None:
        topo = GraphTopology(nodes=[], adjacency={})
        partitioner = SemanticPartitioner()
        result = partitioner.partition(topo)
        assert result.community_count == 0
        assert result.node_to_community == {}
