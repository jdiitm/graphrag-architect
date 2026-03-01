from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.extraction_models import CallsEdge
from orchestrator.app.neo4j_client import GraphRepository, split_hot_targets


def _make_calls_edge(source: str, target: str) -> CallsEdge:
    return CallsEdge(
        source_service_id=source,
        target_service_id=target,
        protocol="http",
        tenant_id="test-tenant",
    )


class TestHotEdgeBatchedWrites:

    def test_hot_edges_detected_above_threshold(self) -> None:
        hot_threshold = 2
        edges = [
            _make_calls_edge("a", "hub"),
            _make_calls_edge("b", "hub"),
            _make_calls_edge("c", "hub"),
        ]
        _, hot = split_hot_targets(edges, threshold=hot_threshold)
        assert len(hot) == 3

    @pytest.mark.asyncio
    async def test_hot_edge_writes_use_batched_unwind(self) -> None:
        session_open_count = 0

        mock_driver = MagicMock()
        mock_session = AsyncMock()

        @asynccontextmanager
        async def counted_session(**kwargs):
            nonlocal session_open_count
            session_open_count += 1
            yield mock_session

        mock_driver.session = counted_session

        repo = GraphRepository(driver=mock_driver)

        hot_edges = [
            _make_calls_edge("a", "hub"),
            _make_calls_edge("b", "hub"),
            _make_calls_edge("c", "hub"),
        ]

        await repo._write_hot_edges_serialized(hot_edges)

        assert session_open_count <= 1, (
            f"Hot edges opened {session_open_count} sessions. "
            f"Must use a single batched UNWIND transaction, not "
            f"one session per edge (causes concurrency collapse on "
            f"high-degree nodes)."
        )
