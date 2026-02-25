import pytest

from orchestrator.app.graph_embeddings import (
    GraphTooLargeError,
    GraphTopology,
    Node2VecConfig,
    Node2VecEmbedder,
    MAX_PYTHON_NODES,
)


class TestNode2VecBounds:
    def test_small_graph_computes_normally(self):
        topology = GraphTopology(
            nodes=["a", "b", "c"],
            adjacency={"a": ["b"], "b": ["c"], "c": ["a"]},
        )
        embedder = Node2VecEmbedder(Node2VecConfig(
            num_walks=2, walk_length=5, embedding_dim=16, seed=42,
        ))
        result = embedder.embed(topology)
        assert len(result) == 3
        assert all(len(v) == 16 for v in result.values())

    def test_large_graph_raises_error(self):
        nodes = [f"n{i}" for i in range(MAX_PYTHON_NODES + 1)]
        topology = GraphTopology(nodes=nodes, adjacency={})
        embedder = Node2VecEmbedder()
        with pytest.raises(GraphTooLargeError, match="exceeds"):
            embedder.embed(topology)

    def test_exactly_at_limit_works(self):
        nodes = [f"n{i}" for i in range(MAX_PYTHON_NODES)]
        topology = GraphTopology(nodes=nodes, adjacency={})
        embedder = Node2VecEmbedder(Node2VecConfig(
            num_walks=1, walk_length=2, embedding_dim=8, seed=42,
        ))
        result = embedder.embed(topology)
        assert len(result) == MAX_PYTHON_NODES

    def test_incremental_also_bounded(self):
        nodes = [f"n{i}" for i in range(MAX_PYTHON_NODES + 1)]
        topology = GraphTopology(nodes=nodes, adjacency={})
        embedder = Node2VecEmbedder()
        with pytest.raises(GraphTooLargeError):
            embedder.embed_incremental(topology, ["n0"], {})
