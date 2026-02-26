from unittest.mock import MagicMock, patch

import pytest

from orchestrator.app.config import Neo4jConfig
from orchestrator.app import neo4j_pool


@pytest.fixture(autouse=True)
def _reset_pool():
    neo4j_pool._state["driver"] = None
    neo4j_pool._state["query_timeout"] = None
    neo4j_pool._state["database"] = None
    yield
    neo4j_pool._state["driver"] = None
    neo4j_pool._state["query_timeout"] = None
    neo4j_pool._state["database"] = None


class TestNeo4jConfigPoolFields:
    def test_default_pool_size(self):
        cfg = Neo4jConfig(
            uri="bolt://host:7687",
            username="neo4j",
            password="pw",
        )
        assert cfg.max_connection_pool_size == 100

    def test_default_acquisition_timeout(self):
        cfg = Neo4jConfig(
            uri="bolt://host:7687",
            username="neo4j",
            password="pw",
        )
        assert cfg.connection_acquisition_timeout == 60.0

    def test_from_env_reads_pool_size(self, monkeypatch):
        monkeypatch.setenv("NEO4J_URI", "bolt://host:7687")
        monkeypatch.setenv("NEO4J_USERNAME", "neo4j")
        monkeypatch.setenv("NEO4J_PASSWORD", "pw")
        monkeypatch.setenv("NEO4J_MAX_CONNECTION_POOL_SIZE", "200")
        cfg = Neo4jConfig.from_env()
        assert cfg.max_connection_pool_size == 200

    def test_from_env_reads_acquisition_timeout(self, monkeypatch):
        monkeypatch.setenv("NEO4J_URI", "bolt://host:7687")
        monkeypatch.setenv("NEO4J_USERNAME", "neo4j")
        monkeypatch.setenv("NEO4J_PASSWORD", "pw")
        monkeypatch.setenv("NEO4J_CONNECTION_ACQUISITION_TIMEOUT", "120")
        cfg = Neo4jConfig.from_env()
        assert cfg.connection_acquisition_timeout == 120.0


class TestInitDriverPassesPoolConfig:
    def test_pool_size_passed_to_driver(self):
        fake_config = MagicMock()
        fake_config.uri = "bolt://host:7687"
        fake_config.username = "neo4j"
        fake_config.password = "pw"
        fake_config.query_timeout = 30.0
        fake_config.database = "neo4j"
        fake_config.max_connection_pool_size = 250
        fake_config.connection_acquisition_timeout = 90.0

        with patch(
            "orchestrator.app.neo4j_pool.Neo4jConfig.from_env",
            return_value=fake_config,
        ), patch(
            "orchestrator.app.neo4j_pool.AsyncGraphDatabase.driver",
            return_value=MagicMock(),
        ) as mock_ctor:
            neo4j_pool.init_driver()

        call_kwargs = mock_ctor.call_args.kwargs
        assert call_kwargs["max_connection_pool_size"] == 250
        assert call_kwargs["connection_acquisition_timeout"] == 90.0
