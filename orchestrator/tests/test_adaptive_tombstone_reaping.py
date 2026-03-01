from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.tombstone_reaper import TombstoneReaper, TombstoneReaperConfig


class TestTombstoneReaperConfigMaxBatch:

    def test_default_max_batch_size(self) -> None:
        config = TombstoneReaperConfig()
        assert config.max_batch_size == 2000

    def test_from_env_reads_max_batch_size(self) -> None:
        env = {"TOMBSTONE_MAX_BATCH_SIZE": "5000"}
        with patch.dict(os.environ, env, clear=False):
            config = TombstoneReaperConfig.from_env()
        assert config.max_batch_size == 5000

    def test_from_env_missing_uses_default(self) -> None:
        env = {k: v for k, v in os.environ.items() if k != "TOMBSTONE_MAX_BATCH_SIZE"}
        with patch.dict(os.environ, env, clear=True):
            config = TombstoneReaperConfig.from_env()
        assert config.max_batch_size == 2000


def _mock_client() -> AsyncMock:
    client = AsyncMock()
    client.count_pending_tombstones = AsyncMock(return_value=0)
    client.reap_tombstone_batch = AsyncMock(return_value=0)
    return client


class TestAdaptiveBatchDoubling:

    @pytest.mark.asyncio
    async def test_unsaturated_batch_stays_at_default(self) -> None:
        client = _mock_client()
        client.reap_tombstone_batch = AsyncMock(
            side_effect=[30, 0],
        )
        config = TombstoneReaperConfig(batch_size=100, max_batch_size=2000)
        reaper = TombstoneReaper(client, config)

        await reaper.reap_once()

        for call in client.reap_tombstone_batch.call_args_list:
            assert call.kwargs["batch_size"] == 100

    @pytest.mark.asyncio
    async def test_saturated_batch_doubles(self) -> None:
        client = _mock_client()
        client.reap_tombstone_batch = AsyncMock(
            side_effect=[100, 200, 50],
        )
        config = TombstoneReaperConfig(batch_size=100, max_batch_size=2000)
        reaper = TombstoneReaper(client, config)

        await reaper.reap_once()

        calls = client.reap_tombstone_batch.call_args_list
        assert calls[0].kwargs["batch_size"] == 100
        assert calls[1].kwargs["batch_size"] == 200

    @pytest.mark.asyncio
    async def test_batch_capped_at_max(self) -> None:
        client = _mock_client()
        client.reap_tombstone_batch = AsyncMock(
            side_effect=[500, 1000, 2000, 100],
        )
        config = TombstoneReaperConfig(batch_size=500, max_batch_size=2000)
        reaper = TombstoneReaper(client, config)

        await reaper.reap_once()

        calls = client.reap_tombstone_batch.call_args_list
        assert calls[0].kwargs["batch_size"] == 500
        assert calls[1].kwargs["batch_size"] == 1000
        assert calls[2].kwargs["batch_size"] == 2000

    @pytest.mark.asyncio
    async def test_doubling_stops_at_ceiling(self) -> None:
        client = _mock_client()
        client.reap_tombstone_batch = AsyncMock(
            side_effect=[200, 400, 400, 100],
        )
        config = TombstoneReaperConfig(batch_size=200, max_batch_size=400)
        reaper = TombstoneReaper(client, config)

        await reaper.reap_once()

        calls = client.reap_tombstone_batch.call_args_list
        assert calls[0].kwargs["batch_size"] == 200
        assert calls[1].kwargs["batch_size"] == 400
        assert calls[2].kwargs["batch_size"] == 400


class TestAdaptiveMetrics:

    @pytest.mark.asyncio
    async def test_last_effective_batch_tracked(self) -> None:
        client = _mock_client()
        client.reap_tombstone_batch = AsyncMock(
            side_effect=[100, 200, 50],
        )
        config = TombstoneReaperConfig(batch_size=100, max_batch_size=2000)
        reaper = TombstoneReaper(client, config)

        await reaper.reap_once()

        assert "last_effective_batch" in reaper.metrics
        assert reaper.metrics["last_effective_batch"] == 400

    @pytest.mark.asyncio
    async def test_last_effective_batch_zero_on_no_reap(self) -> None:
        client = _mock_client()
        client.reap_tombstone_batch = AsyncMock(return_value=0)
        config = TombstoneReaperConfig(batch_size=100, max_batch_size=2000)
        reaper = TombstoneReaper(client, config)

        await reaper.reap_once()

        assert reaper.metrics["last_effective_batch"] == 100
