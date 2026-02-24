from __future__ import annotations

from orchestrator.app.graph_embeddings import (
    GraphTopology,
    Node2VecConfig,
    Node2VecEmbedder,
    generate_walks,
    _biased_random_walk,
    _compute_transition_probs,
    _hash_embedding,
)


def _triangle_topology() -> GraphTopology:
    return GraphTopology(
        nodes=["a", "b", "c"],
        adjacency={
            "a": ["b", "c"],
            "b": ["a", "c"],
            "c": ["a", "b"],
        },
    )


def _line_topology() -> GraphTopology:
    return GraphTopology(
        nodes=["a", "b", "c"],
        adjacency={
            "a": ["b"],
            "b": ["a", "c"],
            "c": ["b"],
        },
    )


class TestGraphTopology:
    def test_neighbors_returns_adjacency(self) -> None:
        topo = _triangle_topology()
        assert set(topo.neighbors("a")) == {"b", "c"}

    def test_neighbors_missing_node_returns_empty(self) -> None:
        topo = _triangle_topology()
        assert topo.neighbors("missing") == []


class TestComputeTransitionProbs:
    def test_no_neighbors_returns_empty(self) -> None:
        topo = _triangle_topology()
        probs = _compute_transition_probs(None, "a", [], topo, 1.0, 1.0)
        assert probs == []

    def test_uniform_p_q_returns_uniform_weights(self) -> None:
        topo = _triangle_topology()
        probs = _compute_transition_probs(None, "a", ["b", "c"], topo, 1.0, 1.0)
        assert len(probs) == 2
        assert abs(sum(probs) - 1.0) < 1e-9

    def test_return_bias_with_low_p(self) -> None:
        topo = _line_topology()
        probs = _compute_transition_probs("a", "b", ["a", "c"], topo, 0.5, 1.0)
        assert probs[0] > probs[1]


class TestBiasedRandomWalk:
    def test_walk_starts_at_start_node(self) -> None:
        import random
        topo = _triangle_topology()
        rng = random.Random(42)
        walk = _biased_random_walk("a", topo, 5, 1.0, 1.0, rng)
        assert walk[0] == "a"

    def test_walk_length_capped(self) -> None:
        import random
        topo = _triangle_topology()
        rng = random.Random(42)
        walk = _biased_random_walk("a", topo, 10, 1.0, 1.0, rng)
        assert len(walk) <= 10

    def test_walk_stops_on_dead_end(self) -> None:
        import random
        topo = GraphTopology(nodes=["isolated"], adjacency={})
        rng = random.Random(42)
        walk = _biased_random_walk("isolated", topo, 5, 1.0, 1.0, rng)
        assert walk == ["isolated"]


class TestGenerateWalks:
    def test_generates_expected_number_of_walks(self) -> None:
        topo = _triangle_topology()
        config = Node2VecConfig(num_walks=3, walk_length=5, seed=42)
        walks = generate_walks(topo, config)
        assert len(walks) == 3 * len(topo.nodes)

    def test_walks_are_deterministic_with_seed(self) -> None:
        topo = _triangle_topology()
        config = Node2VecConfig(num_walks=2, walk_length=5, seed=123)
        walks_a = generate_walks(topo, config)
        walks_b = generate_walks(topo, config)
        assert walks_a == walks_b


class TestHashEmbedding:
    def test_embedding_dimension_matches_config(self) -> None:
        dim = 64
        walks = [["a", "b", "c"]]
        emb = _hash_embedding("a", walks, dim)
        assert len(emb) == dim

    def test_embedding_is_normalized(self) -> None:
        walks = [["a", "b", "a", "c"]]
        emb = _hash_embedding("a", walks, 32)
        norm = sum(v * v for v in emb) ** 0.5
        if norm > 0:
            assert abs(norm - 1.0) < 1e-9

    def test_embedding_zero_for_absent_node(self) -> None:
        walks = [["a", "b", "c"]]
        emb = _hash_embedding("missing", walks, 32)
        assert all(v == 0.0 for v in emb)


class TestNode2VecEmbedder:
    def test_embed_produces_embeddings_for_all_nodes(self) -> None:
        topo = _triangle_topology()
        config = Node2VecConfig(
            num_walks=2, walk_length=5, embedding_dim=32, seed=42,
        )
        embedder = Node2VecEmbedder(config)
        embeddings = embedder.embed(topo)
        assert set(embeddings.keys()) == {"a", "b", "c"}
        for emb in embeddings.values():
            assert len(emb) == 32

    def test_embed_incremental_updates_changed_and_neighbors(self) -> None:
        topo = _triangle_topology()
        config = Node2VecConfig(
            num_walks=2, walk_length=5, embedding_dim=32, seed=42,
        )
        embedder = Node2VecEmbedder(config)
        full = embedder.embed(topo)
        incremental = embedder.embed_incremental(topo, ["a"], full)
        assert set(incremental.keys()) == {"a", "b", "c"}
        assert incremental["a"] != [0.0] * 32

    def test_embed_empty_topology(self) -> None:
        topo = GraphTopology(nodes=[], adjacency={})
        embedder = Node2VecEmbedder()
        result = embedder.embed(topo)
        assert result == {}
