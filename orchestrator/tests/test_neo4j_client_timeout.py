import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.neo4j_client import GraphRepository


def _make_mock_driver() -> AsyncMock:
    mock_driver = AsyncMock()
    mock_session = AsyncMock()
    mock_session.execute_read = AsyncMock(return_value=[])
    mock_session.execute_write = AsyncMock(return_value=None)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    mock_driver.session = MagicMock(return_value=mock_session)
    return mock_driver


class TestGraphRepositoryReadTimeout:
    @pytest.mark.asyncio
    async def test_graph_repository_read_passes_timeout(self) -> None:
        mock_driver = _make_mock_driver()
        mock_session = mock_driver.session.return_value

        repo = GraphRepository(driver=mock_driver, database="neo4j")

        with patch(
            "orchestrator.app.neo4j_client._safe_query_timeout",
            return_value=15.0,
        ):
            await repo.read_topology(label="Service", tenant_id="t1")

        mock_session.execute_read.assert_called_once()
        _, kwargs = mock_session.execute_read.call_args
        assert kwargs["timeout"] == 15.0


class TestGraphRepositoryWriteTimeout:
    @pytest.mark.asyncio
    async def test_graph_repository_write_passes_timeout(self) -> None:
        mock_driver = _make_mock_driver()
        mock_session = mock_driver.session.return_value

        async def _slow_write(*_a: object, **_kw: object) -> None:
            await asyncio.sleep(999)

        mock_session.execute_write = AsyncMock(side_effect=_slow_write)

        repo = GraphRepository(driver=mock_driver, database="neo4j")
        repo._unwind_queries = {str: "UNWIND $batch AS row MERGE (n:Test {id: row.id})"}
        repo._batch_size = 100

        with (
            patch(
                "orchestrator.app.neo4j_client._safe_query_timeout",
                return_value=0.05,
            ),
            pytest.raises(asyncio.TimeoutError),
        ):
            await repo._write_batches(str, [{"id": "1"}])
