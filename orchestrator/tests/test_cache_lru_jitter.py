from __future__ import annotations

import time

import pytest

from orchestrator.app.semantic_cache import CacheConfig, SemanticQueryCache


class TestCacheLRUEviction:

    def test_eviction_removes_least_recently_used(self) -> None:
        config = CacheConfig(max_entries=3, default_ttl_seconds=60.0)
        cache = SemanticQueryCache(config=config)

        cache.store(
            query="first query",
            query_embedding=[1.0, 0.0],
            result={"answer": "first"},
        )
        cache.store(
            query="second query",
            query_embedding=[0.0, 1.0],
            result={"answer": "second"},
        )
        cache.store(
            query="third query",
            query_embedding=[0.5, 0.5],
            result={"answer": "third"},
        )

        cache.lookup([1.0, 0.0])

        cache.store(
            query="fourth query",
            query_embedding=[0.7, 0.3],
            result={"answer": "fourth"},
        )

        first_result = cache.lookup([1.0, 0.0])
        assert first_result is not None, (
            "First entry was recently accessed and should survive LRU eviction"
        )

        second_result = cache.lookup([0.0, 1.0])
        assert second_result is None, (
            "Second entry was least recently used and should be evicted"
        )


class TestCacheTTLJitter:

    def test_entries_have_jittered_ttl(self) -> None:
        config = CacheConfig(default_ttl_seconds=300.0)
        cache = SemanticQueryCache(config=config)

        cache.store(
            query="query one",
            query_embedding=[1.0, 0.0],
            result={"answer": "one"},
        )
        cache.store(
            query="query two",
            query_embedding=[0.0, 1.0],
            result={"answer": "two"},
        )

        entries = list(cache._entries.values())
        assert len(entries) == 2

        ttls = [e.ttl_seconds for e in entries]
        assert ttls[0] != ttls[1], (
            "Cache entries must have jittered TTLs to prevent thundering herd "
            "during mass expiration. Identical TTLs mean no jitter."
        )

    def test_jitter_stays_within_bounds(self) -> None:
        base_ttl = 300.0
        config = CacheConfig(default_ttl_seconds=base_ttl)
        cache = SemanticQueryCache(config=config)

        for i in range(20):
            cache.store(
                query=f"query-{i}",
                query_embedding=[float(i), float(20 - i)],
                result={"answer": f"ans-{i}"},
            )

        for entry in cache._entries.values():
            assert entry.ttl_seconds >= base_ttl * 0.8, (
                f"Jittered TTL {entry.ttl_seconds:.1f}s is below 80% of base {base_ttl}s"
            )
            assert entry.ttl_seconds <= base_ttl * 1.2, (
                f"Jittered TTL {entry.ttl_seconds:.1f}s exceeds 120% of base {base_ttl}s"
            )
