from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app import neo4j_pool


@pytest.fixture(autouse=True)
def _reset_pool():
    neo4j_pool._state["driver"] = None
    yield
    neo4j_pool._state["driver"] = None


class TestGetDriverBeforeInit:
    def test_raises_runtime_error(self):
        with pytest.raises(RuntimeError, match="Neo4j driver not initialized"):
            neo4j_pool.get_driver()


class TestInitDriver:
    def test_creates_driver_from_config(self):
        fake_config = MagicMock()
        fake_config.uri = "bolt://test:7687"
        fake_config.username = "neo4j"
        fake_config.password = "secret"
        fake_config.query_timeout = 42.0

        mock_driver = MagicMock()

        with patch(
            "orchestrator.app.neo4j_pool.Neo4jConfig.from_env",
            return_value=fake_config,
        ), patch(
            "orchestrator.app.neo4j_pool.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ) as mock_ctor:
            neo4j_pool.init_driver()

        mock_ctor.assert_called_once_with(
            "bolt://test:7687",
            auth=("neo4j", "secret"),
            max_transaction_retry_time=42.0,
        )
        assert neo4j_pool._state["driver"] is mock_driver

    def test_configures_query_timeout(self):
        fake_config = MagicMock()
        fake_config.uri = "bolt://host:7687"
        fake_config.username = "neo4j"
        fake_config.password = "pw"
        fake_config.query_timeout = 99.0

        with patch(
            "orchestrator.app.neo4j_pool.Neo4jConfig.from_env",
            return_value=fake_config,
        ), patch(
            "orchestrator.app.neo4j_pool.AsyncGraphDatabase.driver",
            return_value=MagicMock(),
        ) as mock_ctor:
            neo4j_pool.init_driver()

        call_kwargs = mock_ctor.call_args.kwargs
        assert call_kwargs["max_transaction_retry_time"] == 99.0


class TestGetDriverAfterInit:
    def test_returns_singleton_driver(self):
        sentinel = MagicMock()
        neo4j_pool._state["driver"] = sentinel

        result = neo4j_pool.get_driver()

        assert result is sentinel

    def test_returns_same_instance_on_repeated_calls(self):
        sentinel = MagicMock()
        neo4j_pool._state["driver"] = sentinel

        first = neo4j_pool.get_driver()
        second = neo4j_pool.get_driver()

        assert first is second


class TestCloseDriver:
    @pytest.mark.asyncio
    async def test_closes_and_clears_driver(self):
        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()
        neo4j_pool._state["driver"] = mock_driver

        await neo4j_pool.close_driver()

        mock_driver.close.assert_awaited_once()
        assert neo4j_pool._state["driver"] is None

    @pytest.mark.asyncio
    async def test_no_error_when_already_none(self):
        await neo4j_pool.close_driver()
        assert neo4j_pool._state["driver"] is None
