from __future__ import annotations

import pytest

from orchestrator.app.token_bucket import AdaptiveTokenBucket
from orchestrator.app.batched_hop import BatchedHopExecutor, cap_candidates
from orchestrator.app.prompt_registry import PromptRegistry, PromptTemplate


class TestAdaptiveTokenBucketUsable:

    @pytest.mark.asyncio
    async def test_acquire_and_record_success(self) -> None:
        bucket = AdaptiveTokenBucket(capacity=5, refill_rate=5.0)
        await bucket.acquire()
        bucket.record_success()
        assert bucket.available_tokens >= 0

    @pytest.mark.asyncio
    async def test_rate_decreases_on_throttle(self) -> None:
        bucket = AdaptiveTokenBucket(capacity=5, refill_rate=10.0)
        initial_rate = bucket.current_rate
        bucket.record_throttle()
        assert bucket.current_rate < initial_rate


class TestBatchedHopExecutorUsable:

    @pytest.mark.asyncio
    async def test_execute_with_mock_runner(self) -> None:
        results_per_batch = []

        class MockRunner:
            async def run_hop(self, names):
                results_per_batch.append(names)
                return [{"name": n, "score": 1.0} for n in names]

        executor = BatchedHopExecutor(
            runner=MockRunner(), batch_size=3, candidate_limit=10,
        )
        candidates = [{"name": n} for n in ["a", "b", "c", "d", "e"]]
        result = await executor.execute(candidates)
        assert len(results_per_batch) >= 2
        assert len(result) == 5


class TestPromptRegistryUsable:

    def test_register_and_retrieve(self) -> None:
        registry = PromptRegistry()
        template = PromptTemplate(
            name="cypher_gen",
            version="v1",
            system="You generate Cypher queries.",
            human="Generate Cypher for: {query}",
        )
        registry.register(template)
        retrieved = registry.get_latest("cypher_gen")
        assert retrieved.version == "v1"
        assert "Cypher" in retrieved.format_human(query="test")
