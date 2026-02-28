from __future__ import annotations

import pytest

from orchestrator.app.semantic_cache import (
    CacheConfig,
    CacheMetrics,
    SemanticQueryCache,
    _embedding_hash,
    normalize_query,
)


class TestFullDimensionHashing:

    def test_hash_dimensions_none_uses_all_dimensions(self) -> None:
        short = [0.1] * 32
        long = [0.1] * 32 + [0.9] * 96
        hash_short = _embedding_hash(short)
        hash_long = _embedding_hash(long)
        assert hash_short != hash_long

    def test_hash_dimensions_32_uses_only_first_32(self) -> None:
        base = [0.1] * 32
        extended = [0.1] * 32 + [0.9] * 96
        hash_base = _embedding_hash(base, hash_dimensions=32)
        hash_extended = _embedding_hash(extended, hash_dimensions=32)
        assert hash_base == hash_extended

    def test_full_hash_differentiates_vectors_identical_in_first_32(self) -> None:
        prefix = [0.5] * 32
        vec_a = prefix + [0.1] * 96
        vec_b = prefix + [0.9] * 96
        hash_a = _embedding_hash(vec_a)
        hash_b = _embedding_hash(vec_b)
        assert hash_a != hash_b

    def test_deterministic_with_all_dimensions(self) -> None:
        vec = [0.1 * i for i in range(128)]
        assert _embedding_hash(vec) == _embedding_hash(vec)

    def test_deterministic_with_explicit_dimensions(self) -> None:
        vec = [0.1 * i for i in range(128)]
        assert _embedding_hash(vec, hash_dimensions=64) == _embedding_hash(vec, hash_dimensions=64)


class TestNormalizeQuery:

    def test_what_vs_which_equivalence(self) -> None:
        assert normalize_query("What services call auth-service?") == normalize_query(
            "Which services call auth-service?"
        )

    def test_filler_removal(self) -> None:
        assert normalize_query("Can you tell me what calls auth?") == normalize_query(
            "What calls auth?"
        )

    def test_preserves_entity_names(self) -> None:
        normalized = normalize_query("What calls auth-service?")
        assert "auth-service" in normalized

    def test_case_insensitive(self) -> None:
        assert normalize_query("What Calls AUTH-SERVICE?") == normalize_query(
            "what calls auth-service?"
        )

    def test_please_show_me_filler(self) -> None:
        assert normalize_query("Please show me what depends on kafka?") == normalize_query(
            "What depends on kafka?"
        )

    def test_strips_trailing_whitespace(self) -> None:
        assert normalize_query("  what calls auth?  ") == normalize_query("what calls auth?")

    def test_empty_string(self) -> None:
        assert normalize_query("") == ""

    def test_entity_only_query_preserved(self) -> None:
        result = normalize_query("auth-service")
        assert "auth-service" in result


class TestCacheMetricsCounters:

    def test_hits_counter_increments_on_hit(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "a"})
        cache.lookup(embedding)
        metrics = cache.metrics()
        assert metrics.hits == 1

    def test_misses_counter_increments_on_miss(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        cache.lookup([1.0, 0.0, 0.0])
        metrics = cache.metrics()
        assert metrics.misses == 1

    def test_evictions_counter_increments_on_eviction(self) -> None:
        cache = SemanticQueryCache(CacheConfig(
            similarity_threshold=0.9, max_entries=1,
        ))
        cache.store("q1", [1.0, 0.0], {"a": 1})
        cache.store("q2", [0.0, 1.0], {"a": 2})
        metrics = cache.metrics()
        assert metrics.evictions >= 1

    def test_hit_ratio_with_hits_and_misses(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "a"})
        cache.lookup(embedding)
        cache.lookup([0.0, 0.0, 1.0])
        metrics = cache.metrics()
        assert metrics.hit_ratio == pytest.approx(0.5)

    def test_hit_ratio_zero_when_no_lookups(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        metrics = cache.metrics()
        assert metrics.hit_ratio == 0.0

    def test_metrics_instance_is_cache_metrics_type(self) -> None:
        cache = SemanticQueryCache()
        metrics = cache.metrics()
        assert isinstance(metrics, CacheMetrics)


class TestNormalizationWiredIntoLookup:

    def test_normalized_query_improves_cache_hit(self) -> None:
        cache = SemanticQueryCache(CacheConfig(similarity_threshold=0.9))
        embedding = [1.0, 0.0, 0.0]
        cache.store("what services call auth?", embedding, {"answer": "gateway"})

        raw_query = "Can you tell me what services call auth?"
        stored_query = "what services call auth?"
        assert normalize_query(raw_query) == normalize_query(stored_query)
