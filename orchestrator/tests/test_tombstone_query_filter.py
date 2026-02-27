from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.tombstone_filter import (
    TOMBSTONE_CHECK_QUERY,
    TOMBSTONE_CHECK_TENANT_QUERY,
    check_tombstoned_nodes,
    filter_tombstoned_results,
)


def _mock_driver(query_data: list[dict]) -> AsyncMock:
    async def _mock_run(query, **params):
        result = AsyncMock()
        result.data = AsyncMock(return_value=query_data)
        return result

    mock_tx = AsyncMock()
    mock_tx.run = _mock_run

    mock_session = AsyncMock()

    async def _execute_read(func, **kwargs):
        return await func(mock_tx)

    mock_session.execute_read = _execute_read
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    driver = AsyncMock()
    driver.session = MagicMock(return_value=mock_session)
    return driver


class TestTombstoneCheckQuery:
    def test_query_uses_unwind(self) -> None:
        assert "UNWIND" in TOMBSTONE_CHECK_QUERY
        assert "$node_ids" in TOMBSTONE_CHECK_QUERY

    def test_query_filters_tombstoned(self) -> None:
        assert "tombstoned_at IS NOT NULL" in TOMBSTONE_CHECK_QUERY

    def test_tenant_query_includes_tenant_filter(self) -> None:
        assert "$tenant_id" in TOMBSTONE_CHECK_TENANT_QUERY


@pytest.mark.asyncio
class TestCheckTombstonedNodes:
    async def test_returns_tombstoned_ids(self) -> None:
        driver = _mock_driver([
            {"node_id": "svc-stale-1"},
            {"node_id": "svc-stale-2"},
        ])

        result = await check_tombstoned_nodes(
            driver, ["svc-stale-1", "svc-stale-2", "svc-fresh"], "t1",
        )

        assert result == {"svc-stale-1", "svc-stale-2"}

    async def test_returns_empty_for_no_tombstones(self) -> None:
        driver = _mock_driver([])

        result = await check_tombstoned_nodes(
            driver, ["svc-a", "svc-b"], "t1",
        )

        assert result == set()

    async def test_returns_empty_for_empty_input(self) -> None:
        driver = _mock_driver([])

        result = await check_tombstoned_nodes(driver, [], "t1")

        assert result == set()


@pytest.mark.asyncio
class TestFilterTombstonedResults:
    async def test_removes_tombstoned_candidates(self) -> None:
        driver = _mock_driver([{"node_id": "stale-svc"}])

        candidates = [
            {"id": "stale-svc", "name": "stale", "score": 0.9},
            {"id": "fresh-svc", "name": "fresh", "score": 0.8},
        ]

        filtered = await filter_tombstoned_results(
            driver, candidates, "t1",
        )

        assert len(filtered) == 1
        assert filtered[0]["id"] == "fresh-svc"

    async def test_returns_all_when_no_tombstones(self) -> None:
        driver = _mock_driver([])

        candidates = [
            {"id": "svc-a", "score": 0.9},
            {"id": "svc-b", "score": 0.8},
        ]

        filtered = await filter_tombstoned_results(
            driver, candidates, "t1",
        )

        assert len(filtered) == 2

    async def test_returns_empty_for_empty_candidates(self) -> None:
        driver = _mock_driver([])

        filtered = await filter_tombstoned_results(driver, [], "t1")

        assert filtered == []
