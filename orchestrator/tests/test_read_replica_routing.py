from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app import neo4j_pool
from orchestrator.app.config import ReadReplicaConfig


@pytest.fixture(autouse=True)
def _reset_replica_state():
    neo4j_pool._state["driver"] = None
    neo4j_pool._REPLICA_STATE["pool"] = None
    yield
    neo4j_pool._state["driver"] = None
    neo4j_pool._REPLICA_STATE["pool"] = None


class TestReadReplicaConfigDefaults:
    def test_disabled_by_default(self):
        cfg = ReadReplicaConfig()
        assert cfg.enabled is False

    def test_empty_uris_by_default(self):
        cfg = ReadReplicaConfig()
        assert cfg.read_replica_uris == ()

    def test_default_pool_size(self):
        cfg = ReadReplicaConfig()
        assert cfg.read_pool_size == 50


class TestReadReplicaConfigFromEnv:
    def test_reads_enabled_flag(self):
        env = {
            "NEO4J_READ_REPLICA_ENABLED": "true",
            "NEO4J_READ_REPLICA_URIS": "bolt://r1:7687,bolt://r2:7687",
        }
        with patch.dict(os.environ, env, clear=False):
            cfg = ReadReplicaConfig.from_env()
        assert cfg.enabled is True
        assert cfg.read_replica_uris == ("bolt://r1:7687", "bolt://r2:7687")

    def test_reads_pool_size(self):
        env = {
            "NEO4J_READ_REPLICA_ENABLED": "true",
            "NEO4J_READ_REPLICA_URIS": "bolt://r1:7687",
            "NEO4J_READ_REPLICA_POOL_SIZE": "25",
        }
        with patch.dict(os.environ, env, clear=False):
            cfg = ReadReplicaConfig.from_env()
        assert cfg.read_pool_size == 25

    def test_disabled_when_env_not_set(self):
        env_keys = [
            "NEO4J_READ_REPLICA_ENABLED",
            "NEO4J_READ_REPLICA_URIS",
            "NEO4J_READ_REPLICA_POOL_SIZE",
        ]
        cleaned = {k: v for k, v in os.environ.items() if k not in env_keys}
        with patch.dict(os.environ, cleaned, clear=True):
            cfg = ReadReplicaConfig.from_env()
        assert cfg.enabled is False
        assert cfg.read_replica_uris == ()

    def test_empty_uris_string_produces_empty_tuple(self):
        env = {
            "NEO4J_READ_REPLICA_ENABLED": "true",
            "NEO4J_READ_REPLICA_URIS": "",
        }
        with patch.dict(os.environ, env, clear=False):
            cfg = ReadReplicaConfig.from_env()
        assert cfg.read_replica_uris == ()


class TestReplicaAwarePoolFallback:
    def test_returns_primary_when_no_replicas(self):
        from orchestrator.app.neo4j_pool import ReplicaAwarePool

        primary = MagicMock(name="primary_driver")
        pool = ReplicaAwarePool(primary_driver=primary, replica_drivers=())

        assert pool.get_read_driver() is primary

    def test_write_always_returns_primary_with_no_replicas(self):
        from orchestrator.app.neo4j_pool import ReplicaAwarePool

        primary = MagicMock(name="primary_driver")
        pool = ReplicaAwarePool(primary_driver=primary, replica_drivers=())

        assert pool.get_write_driver() is primary


class TestReplicaAwarePoolWithReplicas:
    def test_returns_replica_for_reads(self):
        from orchestrator.app.neo4j_pool import ReplicaAwarePool

        primary = MagicMock(name="primary_driver")
        replica = MagicMock(name="replica_driver")
        pool = ReplicaAwarePool(
            primary_driver=primary,
            replica_drivers=(replica,),
        )

        assert pool.get_read_driver() is replica

    def test_round_robin_across_three_replicas(self):
        from orchestrator.app.neo4j_pool import ReplicaAwarePool

        primary = MagicMock(name="primary")
        r1 = MagicMock(name="r1")
        r2 = MagicMock(name="r2")
        r3 = MagicMock(name="r3")
        pool = ReplicaAwarePool(
            primary_driver=primary,
            replica_drivers=(r1, r2, r3),
        )

        results = [pool.get_read_driver() for _ in range(6)]
        assert results == [r1, r2, r3, r1, r2, r3]

    def test_write_always_returns_primary_regardless_of_replicas(self):
        from orchestrator.app.neo4j_pool import ReplicaAwarePool

        primary = MagicMock(name="primary")
        r1 = MagicMock(name="r1")
        pool = ReplicaAwarePool(
            primary_driver=primary,
            replica_drivers=(r1,),
        )

        for _ in range(5):
            assert pool.get_write_driver() is primary

    def test_single_replica_returns_it_repeatedly(self):
        from orchestrator.app.neo4j_pool import ReplicaAwarePool

        primary = MagicMock(name="primary")
        r1 = MagicMock(name="r1")
        pool = ReplicaAwarePool(
            primary_driver=primary,
            replica_drivers=(r1,),
        )

        results = [pool.get_read_driver() for _ in range(3)]
        assert results == [r1, r1, r1]

    def test_enabled_true_but_empty_uris_falls_back_to_primary(self):
        from orchestrator.app.neo4j_pool import ReplicaAwarePool

        primary = MagicMock(name="primary")
        pool = ReplicaAwarePool(
            primary_driver=primary,
            replica_drivers=(),
        )

        assert pool.get_read_driver() is primary


class TestReplicaAwarePoolCloseAll:
    @pytest.mark.asyncio
    async def test_closes_primary_and_all_replicas(self):
        from orchestrator.app.neo4j_pool import ReplicaAwarePool

        primary = MagicMock()
        primary.close = AsyncMock()
        r1 = MagicMock()
        r1.close = AsyncMock()
        r2 = MagicMock()
        r2.close = AsyncMock()

        pool = ReplicaAwarePool(
            primary_driver=primary,
            replica_drivers=(r1, r2),
        )

        await pool.close_all()

        primary.close.assert_awaited_once()
        r1.close.assert_awaited_once()
        r2.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_closes_primary_only_when_no_replicas(self):
        from orchestrator.app.neo4j_pool import ReplicaAwarePool

        primary = MagicMock()
        primary.close = AsyncMock()
        pool = ReplicaAwarePool(primary_driver=primary, replica_drivers=())

        await pool.close_all()

        primary.close.assert_awaited_once()


class TestModuleLevelGetReadDriver:
    def test_fallback_returns_primary_when_replicas_not_enabled(self):
        sentinel = MagicMock(name="primary_sentinel")
        neo4j_pool._state["driver"] = sentinel

        result = neo4j_pool.get_read_driver()
        assert result is sentinel

    def test_returns_replica_when_pool_configured(self):
        primary = MagicMock(name="primary")
        replica = MagicMock(name="replica")
        pool = neo4j_pool.ReplicaAwarePool(
            primary_driver=primary,
            replica_drivers=(replica,),
        )
        neo4j_pool._REPLICA_STATE["pool"] = pool

        result = neo4j_pool.get_read_driver()
        assert result is replica


class TestQueryEngineReadReplicaRouting:
    def test_get_neo4j_driver_calls_get_read_driver(self):
        from orchestrator.app.query_engine import _get_neo4j_driver

        sentinel = MagicMock(name="read_replica_sentinel")
        with patch(
            "orchestrator.app.query_engine.get_read_driver",
            return_value=sentinel,
        ) as mock_get_read:
            result = _get_neo4j_driver()
        mock_get_read.assert_called_once()
        assert result is sentinel

    def test_get_neo4j_driver_does_not_call_get_driver(self):
        from orchestrator.app.query_engine import _get_neo4j_driver

        with (
            patch("orchestrator.app.query_engine.get_read_driver", return_value=MagicMock()),
            patch("orchestrator.app.query_engine.get_driver") as mock_write,
        ):
            _get_neo4j_driver()
        mock_write.assert_not_called()

    @pytest.mark.asyncio
    async def test_neo4j_session_yields_read_driver_when_no_tenant(self):
        from orchestrator.app.query_engine import _neo4j_session

        sentinel = MagicMock(name="read_driver")
        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=sentinel,
        ):
            async with _neo4j_session() as driver:
                assert driver is sentinel

    def test_get_neo4j_write_driver_calls_get_driver(self):
        from orchestrator.app.query_engine import _get_neo4j_write_driver

        sentinel = MagicMock(name="primary_sentinel")
        with patch(
            "orchestrator.app.query_engine.get_driver",
            return_value=sentinel,
        ) as mock_get:
            result = _get_neo4j_write_driver()
        mock_get.assert_called_once()
        assert result is sentinel
