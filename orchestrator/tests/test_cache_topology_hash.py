from __future__ import annotations

import pytest

from orchestrator.app.semantic_cache import (
    CacheConfig,
    SemanticQueryCache,
    compute_topology_hash,
)


class TestTopologyHashComputation:

    def test_deterministic_for_same_node_ids(self) -> None:
        ids_a = {"svc-auth", "svc-gateway", "db-users"}
        ids_b = {"db-users", "svc-gateway", "svc-auth"}
        assert compute_topology_hash(ids_a) == compute_topology_hash(ids_b)

    def test_different_for_different_node_ids(self) -> None:
        ids_a = {"svc-auth", "svc-gateway"}
        ids_b = {"svc-auth", "svc-orders"}
        assert compute_topology_hash(ids_a) != compute_topology_hash(ids_b)

    def test_empty_set_produces_stable_hash(self) -> None:
        h1 = compute_topology_hash(set())
        h2 = compute_topology_hash(set())
        assert h1 == h2
        assert isinstance(h1, str)
        assert len(h1) > 0


class TestCacheStoreWithTopologyHash:

    def test_store_records_topology_hash_when_node_ids_provided(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        node_ids = {"svc-auth", "svc-gateway"}

        cache.store(
            "what calls auth?", embedding, {"answer": "gateway"},
            node_ids=node_ids,
        )

        entry = list(cache._entries.values())[0]
        expected_hash = compute_topology_hash(node_ids)
        assert entry.topology_hash == expected_hash

    def test_store_without_node_ids_has_empty_topology_hash(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]

        cache.store("q", embedding, {"answer": "x"})

        entry = list(cache._entries.values())[0]
        assert entry.topology_hash == ""


class TestCacheValidateTopologyHash:

    def test_validate_returns_true_when_hash_matches(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        node_ids = {"svc-auth", "svc-gateway"}
        cache.store("q", embedding, {"answer": "ok"}, node_ids=node_ids)

        is_valid = cache.validate_topology(embedding, current_node_ids=node_ids)
        assert is_valid is True

    def test_validate_returns_false_when_topology_drifted(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        original_ids = {"svc-auth", "svc-gateway"}
        cache.store("q", embedding, {"answer": "stale"}, node_ids=original_ids)

        drifted_ids = {"svc-auth", "svc-orders"}
        is_valid = cache.validate_topology(embedding, current_node_ids=drifted_ids)
        assert is_valid is False

    def test_validate_returns_true_for_entry_without_topology_hash(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "x"})

        is_valid = cache.validate_topology(
            embedding, current_node_ids={"svc-any"},
        )
        assert is_valid is True

    def test_validate_returns_true_when_no_matching_entry(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        is_valid = cache.validate_topology(
            [0.0, 1.0, 0.0], current_node_ids={"svc-any"},
        )
        assert is_valid is True


class TestCacheInvalidateStaleTopology:

    def test_invalidate_stale_topology_removes_drifted_entries(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding_a = [1.0, 0.0, 0.0]
        embedding_b = [0.0, 1.0, 0.0]

        cache.store(
            "q1", embedding_a, {"answer": "stale"},
            node_ids={"svc-auth", "svc-gateway"},
        )
        cache.store(
            "q2", embedding_b, {"answer": "fresh"},
            node_ids={"svc-orders"},
        )

        current_graph_nodes = {"svc-auth", "svc-orders"}

        removed = cache.invalidate_stale_topologies(current_graph_nodes)

        assert removed == 1
        assert cache.stats().size == 1
        assert cache.lookup(embedding_b) == {"answer": "fresh"}
        assert cache.lookup(embedding_a) is None
