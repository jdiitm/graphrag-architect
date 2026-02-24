from __future__ import annotations

from typing import Any, Dict, List

import pytest

from orchestrator.app.topological_evaluator import (
    TopologicalEvaluator,
    TopologicalScore,
)


class TestEdgeExistenceVerification:

    @pytest.mark.asyncio
    async def test_all_edges_exist_returns_full_score(self) -> None:
        async def _mock_verify(edge_ids: List[str]) -> int:
            return len(edge_ids)

        evaluator = TopologicalEvaluator(verify_edges=_mock_verify)
        score = await evaluator.evaluate_topology(
            claimed_edge_ids=["e1", "e2", "e3"],
        )
        assert score.edge_existence_ratio == 1.0

    @pytest.mark.asyncio
    async def test_some_edges_missing_returns_partial_score(self) -> None:
        async def _mock_verify(edge_ids: List[str]) -> int:
            return 1

        evaluator = TopologicalEvaluator(verify_edges=_mock_verify)
        score = await evaluator.evaluate_topology(
            claimed_edge_ids=["e1", "e2", "e3"],
        )
        assert abs(score.edge_existence_ratio - 1.0 / 3.0) < 0.01

    @pytest.mark.asyncio
    async def test_no_edges_returns_zero(self) -> None:
        async def _mock_verify(edge_ids: List[str]) -> int:
            return 0

        evaluator = TopologicalEvaluator(verify_edges=_mock_verify)
        score = await evaluator.evaluate_topology(
            claimed_edge_ids=[],
        )
        assert score.edge_existence_ratio == 0.0


class TestPathReachability:

    @pytest.mark.asyncio
    async def test_reachable_path_scores_one(self) -> None:
        async def _mock_verify(edge_ids: List[str]) -> int:
            return len(edge_ids)

        async def _mock_path(start: str, end: str, max_hops: int) -> bool:
            return True

        evaluator = TopologicalEvaluator(
            verify_edges=_mock_verify,
            check_path_reachability=_mock_path,
        )
        score = await evaluator.evaluate_topology(
            claimed_edge_ids=["e1"],
            start_node="a",
            end_node="b",
        )
        assert score.path_reachable is True

    @pytest.mark.asyncio
    async def test_unreachable_path_scores_zero(self) -> None:
        async def _mock_verify(edge_ids: List[str]) -> int:
            return len(edge_ids)

        async def _mock_path(start: str, end: str, max_hops: int) -> bool:
            return False

        evaluator = TopologicalEvaluator(
            verify_edges=_mock_verify,
            check_path_reachability=_mock_path,
        )
        score = await evaluator.evaluate_topology(
            claimed_edge_ids=["e1"],
            start_node="a",
            end_node="b",
        )
        assert score.path_reachable is False


class TestCompositeScoring:

    @pytest.mark.asyncio
    async def test_composite_score_blends_vector_and_topo(self) -> None:
        async def _mock_verify(edge_ids: List[str]) -> int:
            return len(edge_ids)

        evaluator = TopologicalEvaluator(
            verify_edges=_mock_verify,
            alpha=0.6,
        )
        score = await evaluator.evaluate_topology(
            claimed_edge_ids=["e1", "e2"],
            vector_score=0.8,
        )
        expected = 0.6 * 0.8 + 0.4 * 1.0
        assert abs(score.composite_score - expected) < 0.01

    @pytest.mark.asyncio
    async def test_hallucinated_path_flagged(self) -> None:
        async def _mock_verify(edge_ids: List[str]) -> int:
            return 0

        evaluator = TopologicalEvaluator(
            verify_edges=_mock_verify,
            topo_threshold=0.3,
        )
        score = await evaluator.evaluate_topology(
            claimed_edge_ids=["e1", "e2"],
        )
        assert score.is_hallucinated is True
        assert score.edge_existence_ratio == 0.0

    @pytest.mark.asyncio
    async def test_valid_path_not_flagged(self) -> None:
        async def _mock_verify(edge_ids: List[str]) -> int:
            return len(edge_ids)

        evaluator = TopologicalEvaluator(
            verify_edges=_mock_verify,
            topo_threshold=0.3,
        )
        score = await evaluator.evaluate_topology(
            claimed_edge_ids=["e1", "e2"],
        )
        assert score.is_hallucinated is False
