from __future__ import annotations

import asyncio

import pytest

from orchestrator.app.lazy_traversal import (
    GraphTooLargeForLocalPPR,
    LocalPageRankStrategy,
    _LOCAL_SAFETY_CAP,
    gds_pagerank_filter,
)


def _make_edges(count: int) -> list[dict]:
    return [
        {"source": f"svc-{i}", "target": f"svc-{i + 1}"}
        for i in range(count)
    ]


class TestLocalPageRankSafetyCap:

    def test_raises_on_graph_exceeding_safety_cap(self) -> None:
        strategy = LocalPageRankStrategy()
        oversized = _make_edges(_LOCAL_SAFETY_CAP + 1)
        with pytest.raises(GraphTooLargeForLocalPPR):
            strategy.rank(oversized, ["svc-0"])

    def test_succeeds_at_safety_cap_boundary(self) -> None:
        strategy = LocalPageRankStrategy(max_edges=100)
        edges = _make_edges(100)
        result = strategy.rank(edges, ["svc-0"], top_n=5)
        assert len(result) > 0

    def test_succeeds_for_small_graph(self) -> None:
        strategy = LocalPageRankStrategy()
        edges = _make_edges(10)
        result = strategy.rank(edges, ["svc-0"], top_n=5)
        assert len(result) > 0


class TestGdsPageRankFilterOffloading:

    @pytest.mark.asyncio
    async def test_gds_filter_offloads_local_strategy_to_thread(self) -> None:
        edges = _make_edges(50)
        result = await gds_pagerank_filter(
            driver=None,
            hop_records=edges,
            seed_names=["svc-0"],
            top_n=10,
        )
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_gds_filter_does_not_block_event_loop(self) -> None:
        edges = _make_edges(200)

        canary_ran = False

        async def canary_task() -> None:
            nonlocal canary_ran
            await asyncio.sleep(0)
            canary_ran = True

        canary = asyncio.create_task(canary_task())
        result = await gds_pagerank_filter(
            driver=None,
            hop_records=edges,
            seed_names=["svc-0"],
            top_n=10,
        )
        await canary
        assert canary_ran, (
            "Canary task must run during gds_pagerank_filter, "
            "proving the event loop was not blocked"
        )
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_gds_filter_handles_oversized_graph_gracefully(self) -> None:
        edges = _make_edges(_LOCAL_SAFETY_CAP + 100)
        result = await gds_pagerank_filter(
            driver=None,
            hop_records=edges,
            seed_names=["svc-0"],
            top_n=10,
        )
        assert isinstance(result, list)
