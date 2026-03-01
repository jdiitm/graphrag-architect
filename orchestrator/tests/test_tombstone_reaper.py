from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.tombstone_reaper import TombstoneReaper, TombstoneReaperConfig


class TestTombstoneReaperConfigDefaults:

    def test_default_ttl_days(self) -> None:
        config = TombstoneReaperConfig()
        assert config.ttl_days == 7

    def test_default_batch_size(self) -> None:
        config = TombstoneReaperConfig()
        assert config.batch_size == 100

    def test_default_reap_interval_seconds(self) -> None:
        config = TombstoneReaperConfig()
        assert config.reap_interval_seconds == 3600

    def test_frozen(self) -> None:
        config = TombstoneReaperConfig()
        with pytest.raises(AttributeError):
            config.ttl_days = 99  # type: ignore[misc]


class TestTombstoneReaperConfigFromEnv:

    def test_reads_env_vars(self) -> None:
        env = {
            "TOMBSTONE_TTL_DAYS": "14",
            "TOMBSTONE_BATCH_SIZE": "200",
            "TOMBSTONE_REAP_INTERVAL_SECONDS": "1800",
        }
        with patch.dict(os.environ, env, clear=False):
            config = TombstoneReaperConfig.from_env()
        assert config.ttl_days == 14
        assert config.batch_size == 200
        assert config.reap_interval_seconds == 1800

    def test_falls_back_to_defaults(self) -> None:
        env = {
            "TOMBSTONE_TTL_DAYS": "",
            "TOMBSTONE_BATCH_SIZE": "",
            "TOMBSTONE_REAP_INTERVAL_SECONDS": "",
        }
        with patch.dict(os.environ, env, clear=False):
            config = TombstoneReaperConfig.from_env()
        assert config.ttl_days == 7
        assert config.batch_size == 100
        assert config.reap_interval_seconds == 3600


def _mock_neo4j_client() -> AsyncMock:
    client = AsyncMock()
    client.count_pending_tombstones = AsyncMock(return_value=0)
    client.reap_tombstone_batch = AsyncMock(return_value=0)
    return client


class TestReaperReapsExpiredEdges:

    @pytest.mark.asyncio
    async def test_deletes_edges_older_than_ttl(self) -> None:
        client = _mock_neo4j_client()
        client.reap_tombstone_batch = AsyncMock(
            side_effect=[50, 0],
        )
        config = TombstoneReaperConfig(ttl_days=7, batch_size=100)
        reaper = TombstoneReaper(client, config)

        reaped = await reaper.reap_once()

        assert reaped == 50
        assert client.reap_tombstone_batch.call_count == 1

    @pytest.mark.asyncio
    async def test_preserves_edges_younger_than_ttl(self) -> None:
        client = _mock_neo4j_client()
        client.reap_tombstone_batch = AsyncMock(return_value=0)
        config = TombstoneReaperConfig(ttl_days=7, batch_size=100)
        reaper = TombstoneReaper(client, config)

        reaped = await reaper.reap_once()

        assert reaped == 0
        assert client.reap_tombstone_batch.call_count == 1


class TestReaperBatchProcessing:

    @pytest.mark.asyncio
    async def test_processes_with_adaptive_batch_sizing(self) -> None:
        client = _mock_neo4j_client()
        client.reap_tombstone_batch = AsyncMock(
            side_effect=[50, 100, 30],
        )
        config = TombstoneReaperConfig(batch_size=50)
        reaper = TombstoneReaper(client, config)

        reaped = await reaper.reap_once()

        assert reaped == 180
        assert client.reap_tombstone_batch.call_count == 3
        calls = client.reap_tombstone_batch.call_args_list
        assert calls[0].kwargs["batch_size"] == 50
        assert calls[1].kwargs["batch_size"] == 100
        assert calls[2].kwargs["batch_size"] == 200

    @pytest.mark.asyncio
    async def test_stops_when_batch_returns_less_than_batch_size(self) -> None:
        client = _mock_neo4j_client()
        client.reap_tombstone_batch = AsyncMock(
            side_effect=[100, 42],
        )
        config = TombstoneReaperConfig(batch_size=100)
        reaper = TombstoneReaper(client, config)

        reaped = await reaper.reap_once()

        assert reaped == 142
        assert client.reap_tombstone_batch.call_count == 2


class TestReaperPeriodicExecution:

    @pytest.mark.asyncio
    async def test_runs_periodically_at_configured_interval(self) -> None:
        client = _mock_neo4j_client()
        config = TombstoneReaperConfig(reap_interval_seconds=0.05)
        reaper = TombstoneReaper(client, config)

        reaper.start()
        await asyncio.sleep(0.18)
        await reaper.stop()

        assert client.reap_tombstone_batch.call_count >= 2


class TestReaperGracefulShutdown:

    @pytest.mark.asyncio
    async def test_stop_cancels_background_task(self) -> None:
        client = _mock_neo4j_client()
        config = TombstoneReaperConfig(reap_interval_seconds=600)
        reaper = TombstoneReaper(client, config)

        reaper.start()
        assert reaper.running
        await reaper.stop()
        assert not reaper.running

    @pytest.mark.asyncio
    async def test_stop_is_idempotent(self) -> None:
        client = _mock_neo4j_client()
        config = TombstoneReaperConfig(reap_interval_seconds=600)
        reaper = TombstoneReaper(client, config)

        reaper.start()
        await reaper.stop()
        await reaper.stop()
        assert not reaper.running


class TestReaperMetrics:

    @pytest.mark.asyncio
    async def test_reaped_total_increments(self) -> None:
        client = _mock_neo4j_client()
        client.reap_tombstone_batch = AsyncMock(side_effect=[25, 0])
        config = TombstoneReaperConfig()
        reaper = TombstoneReaper(client, config)

        await reaper.reap_once()

        assert reaper.metrics["reaped_total"] == 25

    @pytest.mark.asyncio
    async def test_reaped_total_accumulates_across_runs(self) -> None:
        client = _mock_neo4j_client()
        client.reap_tombstone_batch = AsyncMock(
            side_effect=[10, 15],
        )
        config = TombstoneReaperConfig()
        reaper = TombstoneReaper(client, config)

        await reaper.reap_once()
        await reaper.reap_once()

        assert reaper.metrics["reaped_total"] == 25

    @pytest.mark.asyncio
    async def test_pending_reflects_count(self) -> None:
        client = _mock_neo4j_client()
        client.count_pending_tombstones = AsyncMock(return_value=42)
        config = TombstoneReaperConfig()
        reaper = TombstoneReaper(client, config)

        await reaper.reap_once()

        assert reaper.metrics["pending"] == 42


class TestReaperEmptyDatabase:

    @pytest.mark.asyncio
    async def test_no_tombstones_does_nothing(self) -> None:
        client = _mock_neo4j_client()
        config = TombstoneReaperConfig()
        reaper = TombstoneReaper(client, config)

        reaped = await reaper.reap_once()

        assert reaped == 0
        assert reaper.metrics["reaped_total"] == 0
        assert reaper.metrics["pending"] == 0


class TestReaperTenantIsolation:

    @pytest.mark.asyncio
    async def test_passes_tenant_id_to_queries(self) -> None:
        client = _mock_neo4j_client()
        config = TombstoneReaperConfig()
        reaper = TombstoneReaper(client, config, tenant_id="acme-corp")

        await reaper.reap_once()

        reap_kwargs = client.reap_tombstone_batch.call_args_list[0].kwargs
        assert reap_kwargs["tenant_id"] == "acme-corp"

        count_kwargs = client.count_pending_tombstones.call_args_list[0].kwargs
        assert count_kwargs["tenant_id"] == "acme-corp"

    @pytest.mark.asyncio
    async def test_empty_tenant_id_queries_all(self) -> None:
        client = _mock_neo4j_client()
        config = TombstoneReaperConfig()
        reaper = TombstoneReaper(client, config, tenant_id="")

        await reaper.reap_once()

        reap_kwargs = client.reap_tombstone_batch.call_args_list[0].kwargs
        assert reap_kwargs["tenant_id"] == ""


class TestReaperCutoffCalculation:

    @pytest.mark.asyncio
    async def test_cutoff_is_now_minus_ttl_days(self) -> None:
        client = _mock_neo4j_client()
        config = TombstoneReaperConfig(ttl_days=14)
        reaper = TombstoneReaper(client, config)

        fixed_now = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        expected_cutoff = (fixed_now - timedelta(days=14)).isoformat()

        with patch("orchestrator.app.tombstone_reaper.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            await reaper.reap_once()

        reap_kwargs = client.reap_tombstone_batch.call_args_list[0].kwargs
        assert reap_kwargs["cutoff"] == expected_cutoff
