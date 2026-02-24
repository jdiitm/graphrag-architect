from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import pytest

from orchestrator.app.batched_hop import (
    BatchedHopExecutor,
    cap_candidates,
    partition_names,
)


class TestCapCandidates:
    def test_caps_at_limit(self) -> None:
        candidates = [{"name": f"svc-{i}"} for i in range(100)]
        result = cap_candidates(candidates, limit=50)
        assert len(result) == 50

    def test_returns_all_when_under_limit(self) -> None:
        candidates = [{"name": "a"}, {"name": "b"}]
        result = cap_candidates(candidates, limit=50)
        assert len(result) == 2

    def test_empty_input(self) -> None:
        assert cap_candidates([], limit=50) == []


class TestScoreRankedCapping:

    def test_highest_scores_survive_cap(self) -> None:
        candidates = [
            {"name": f"svc-{i}", "score": float(i)} for i in range(10)
        ]
        result = cap_candidates(candidates, limit=3)
        assert len(result) == 3
        scores = [c["score"] for c in result]
        assert scores == sorted(scores, reverse=True), (
            "cap_candidates must return highest-scored candidates, "
            f"got scores: {scores}"
        )

    def test_score_ranked_cap_preserves_top(self) -> None:
        candidates = [
            {"name": "low", "score": 0.1},
            {"name": "high", "score": 0.9},
            {"name": "mid", "score": 0.5},
        ]
        result = cap_candidates(candidates, limit=2)
        names = [c["name"] for c in result]
        assert "high" in names
        assert "mid" in names
        assert "low" not in names

    def test_candidates_without_score_still_capped(self) -> None:
        candidates = [{"name": f"svc-{i}"} for i in range(10)]
        result = cap_candidates(candidates, limit=5)
        assert len(result) == 5

    def test_configurable_limit_via_env(self) -> None:
        import os
        os.environ["CANDIDATE_LIMIT"] = "7"
        try:
            from orchestrator.app.batched_hop import default_candidate_limit
            assert default_candidate_limit() == 7
        finally:
            del os.environ["CANDIDATE_LIMIT"]


class TestPartitionNames:
    def test_partitions_evenly(self) -> None:
        names = [f"svc-{i}" for i in range(10)]
        batches = partition_names(names, batch_size=5)
        assert len(batches) == 2
        assert len(batches[0]) == 5
        assert len(batches[1]) == 5

    def test_remainder_batch(self) -> None:
        names = [f"svc-{i}" for i in range(7)]
        batches = partition_names(names, batch_size=3)
        assert len(batches) == 3
        assert len(batches[2]) == 1

    def test_single_batch(self) -> None:
        names = ["a", "b"]
        batches = partition_names(names, batch_size=10)
        assert len(batches) == 1

    def test_empty_names(self) -> None:
        assert partition_names([], batch_size=5) == []


class FakeHopRunner:
    def __init__(self, results: Dict[str, List[Dict[str, Any]]]) -> None:
        self._results = results
        self.queries_run: List[List[str]] = []

    async def run_hop(self, names: List[str]) -> List[Dict[str, Any]]:
        self.queries_run.append(names)
        result: List[Dict[str, Any]] = []
        for name in names:
            result.extend(self._results.get(name, []))
        return result


class TestBatchedHopExecutor:
    def test_single_batch_returns_results(self) -> None:
        runner = FakeHopRunner({
            "auth": [{"source": "auth", "rel": "CALLS", "target": "db"}],
        })
        executor = BatchedHopExecutor(
            runner, candidate_limit=50, batch_size=10,
        )
        candidates = [{"name": "auth"}]

        result = asyncio.run(executor.execute(candidates))
        assert len(result) == 1
        assert result[0]["source"] == "auth"

    def test_large_candidates_partitioned(self) -> None:
        data = {
            f"svc-{i}": [{"source": f"svc-{i}", "rel": "CALLS", "target": "x"}]
            for i in range(100)
        }
        runner = FakeHopRunner(data)
        executor = BatchedHopExecutor(
            runner, candidate_limit=50, batch_size=10,
        )
        candidates = [{"name": f"svc-{i}"} for i in range(100)]

        result = asyncio.run(executor.execute(candidates))
        assert len(result) == 50
        assert len(runner.queries_run) == 5

    def test_deduplicates_cross_batch_results(self) -> None:
        data = {
            "a": [{"source": "a", "rel": "CALLS", "target": "shared"}],
            "b": [{"source": "a", "rel": "CALLS", "target": "shared"}],
        }
        runner = FakeHopRunner(data)
        executor = BatchedHopExecutor(
            runner, candidate_limit=50, batch_size=1,
        )
        candidates = [{"name": "a"}, {"name": "b"}]

        result = asyncio.run(executor.execute(candidates))
        assert len(result) == 1

    def test_empty_candidates(self) -> None:
        runner = FakeHopRunner({})
        executor = BatchedHopExecutor(runner, candidate_limit=50, batch_size=10)

        result = asyncio.run(executor.execute([]))
        assert result == []


class TestHashBasedDedup:

    def test_no_json_dumps_in_dedup(self) -> None:
        import inspect
        from orchestrator.app.batched_hop import BatchedHopExecutor as BHE
        source = inspect.getsource(BHE.execute)
        assert "json.dumps" not in source, (
            "Deduplication must NOT use json.dumps (CPU-bound). "
            "Use hash-based dedup on identity fields instead."
        )

    def test_dedup_by_identity_fields(self) -> None:
        data = {
            "a": [
                {"source": "a", "rel": "CALLS", "target": "shared", "extra": 1},
            ],
            "b": [
                {"source": "a", "rel": "CALLS", "target": "shared", "extra": 2},
            ],
        }
        runner = FakeHopRunner(data)
        executor = BatchedHopExecutor(
            runner, candidate_limit=50, batch_size=1,
        )
        candidates = [{"name": "a"}, {"name": "b"}]
        result = asyncio.run(executor.execute(candidates))
        assert len(result) == 1, (
            "Records with same identity fields (source, rel, target) "
            "but different extra fields must be deduplicated"
        )
