from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import pytest

from orchestrator.app.semantic_cache import (
    CacheConfig,
    SemanticQueryCache,
)


@pytest.fixture()
def cache() -> SemanticQueryCache:
    return SemanticQueryCache(CacheConfig(similarity_threshold=0.9))


class TestSingleflightCoalescing:

    @pytest.mark.asyncio
    async def test_concurrent_lookups_coalesce_to_one_compute(
        self, cache: SemanticQueryCache,
    ) -> None:
        embedding = [1.0, 0.0, 0.0]
        compute_count = 0

        async def simulate_compute(emb: List[float], tid: str) -> Dict[str, Any]:
            nonlocal compute_count
            result, is_owner = await cache.lookup_or_wait(emb, tenant_id=tid)
            if result is not None:
                return result
            compute_count += 1
            await asyncio.sleep(0.05)
            answer: Dict[str, Any] = {"answer": "coalesced"}
            cache.store("q", emb, answer, tenant_id=tid)
            cache.notify_complete(emb)
            return answer

        tasks = [simulate_compute(embedding, "") for _ in range(50)]
        results = await asyncio.gather(*tasks)

        assert compute_count == 1
        assert all(r == {"answer": "coalesced"} for r in results)

    @pytest.mark.asyncio
    async def test_owner_failure_releases_waiters(
        self, cache: SemanticQueryCache,
    ) -> None:
        embedding = [1.0, 0.0, 0.0]
        calls: List[str] = []

        async def failing_owner() -> Optional[Dict[str, Any]]:
            result, is_owner = await cache.lookup_or_wait(embedding, tenant_id="")
            if result is not None:
                return result
            calls.append("owner_started")
            await asyncio.sleep(0.02)
            cache.notify_complete(embedding, failed=True)
            return None

        async def waiter() -> Optional[Dict[str, Any]]:
            await asyncio.sleep(0.005)
            result, is_owner = await cache.lookup_or_wait(embedding, tenant_id="")
            if result is not None:
                return result
            calls.append("waiter_became_owner")
            answer: Dict[str, Any] = {"answer": "recovered"}
            cache.store("q", embedding, answer)
            cache.notify_complete(embedding)
            return answer

        owner_task = asyncio.create_task(failing_owner())
        waiter_task = asyncio.create_task(waiter())

        owner_result, waiter_result = await asyncio.gather(
            owner_task, waiter_task,
        )

        assert owner_result is None
        assert waiter_result == {"answer": "recovered"}
        assert "owner_started" in calls
        assert "waiter_became_owner" in calls

    @pytest.mark.asyncio
    async def test_different_embeddings_do_not_block(
        self, cache: SemanticQueryCache,
    ) -> None:
        emb_a = [1.0, 0.0, 0.0]
        emb_b = [0.0, 1.0, 0.0]
        compute_order: List[str] = []

        async def compute_a() -> None:
            result, is_owner = await cache.lookup_or_wait(emb_a, tenant_id="")
            if result is None:
                compute_order.append("a_start")
                await asyncio.sleep(0.05)
                cache.store("qa", emb_a, {"a": True})
                cache.notify_complete(emb_a)
                compute_order.append("a_end")

        async def compute_b() -> None:
            result, is_owner = await cache.lookup_or_wait(emb_b, tenant_id="")
            if result is None:
                compute_order.append("b_start")
                await asyncio.sleep(0.01)
                cache.store("qb", emb_b, {"b": True})
                cache.notify_complete(emb_b)
                compute_order.append("b_end")

        await asyncio.gather(compute_a(), compute_b())

        assert "a_start" in compute_order
        assert "b_start" in compute_order
        b_end_idx = compute_order.index("b_end")
        a_end_idx = compute_order.index("a_end")
        assert b_end_idx < a_end_idx

    @pytest.mark.asyncio
    async def test_lookup_or_wait_returns_cached_result(
        self, cache: SemanticQueryCache,
    ) -> None:
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "pre-cached"})
        result, is_owner = await cache.lookup_or_wait(embedding, tenant_id="")
        assert result == {"answer": "pre-cached"}
        assert is_owner is False

    @pytest.mark.asyncio
    async def test_tenant_isolation_during_coalescing(
        self, cache: SemanticQueryCache,
    ) -> None:
        embedding = [1.0, 0.0, 0.0]
        cache.store("q", embedding, {"answer": "t1_data"}, tenant_id="t1")

        result, is_owner = await cache.lookup_or_wait(embedding, tenant_id="t1")
        assert result == {"answer": "t1_data"}
        assert is_owner is False

        result2, is_owner2 = await cache.lookup_or_wait(embedding, tenant_id="t2")
        assert result2 is None
        assert is_owner2 is True
        cache.notify_complete(embedding)
