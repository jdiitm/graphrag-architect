from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.graph_embeddings import (
    GDSProjectionManager,
    GraphTooLargeError,
    ProjectionGuardConfig,
)


class TestProjectionGuardConfig:
    def test_defaults(self) -> None:
        cfg = ProjectionGuardConfig()
        assert cfg.max_projection_nodes == 500_000
        assert cfg.max_concurrent_projections == 3
        assert cfg.estimation_query_timeout == 5.0

    def test_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GDS_MAX_PROJECTION_NODES", "100000")
        monkeypatch.setenv("GDS_MAX_CONCURRENT_PROJECTIONS", "5")
        monkeypatch.setenv("GDS_ESTIMATION_QUERY_TIMEOUT", "10.0")
        cfg = ProjectionGuardConfig.from_env()
        assert cfg.max_projection_nodes == 100_000
        assert cfg.max_concurrent_projections == 5
        assert cfg.estimation_query_timeout == 10.0

    def test_frozen(self) -> None:
        cfg = ProjectionGuardConfig()
        with pytest.raises(AttributeError):
            cfg.max_projection_nodes = 10


class TestGDSProjectionGuard:
    @pytest.fixture
    def mock_redis(self) -> AsyncMock:
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        redis.set = AsyncMock()
        redis.delete = AsyncMock()
        redis.incr = AsyncMock()
        return redis

    @pytest.fixture
    def mock_driver(self) -> MagicMock:
        driver = MagicMock()
        session = AsyncMock()
        session.execute_write = AsyncMock()
        session.execute_read = AsyncMock(return_value=100)

        async def session_ctx(**kwargs):
            return session

        driver.session = MagicMock(return_value=session)
        driver.session.return_value.__aenter__ = AsyncMock(return_value=session)
        driver.session.return_value.__aexit__ = AsyncMock(return_value=False)
        return driver

    @pytest.mark.asyncio
    async def test_projection_rejected_when_too_large(
        self, mock_driver: MagicMock, mock_redis: AsyncMock,
    ) -> None:
        guard_cfg = ProjectionGuardConfig(max_projection_nodes=50)
        mgr = GDSProjectionManager(
            driver=mock_driver,
            redis_conn=mock_redis,
            guard_config=guard_cfg,
        )
        mgr._estimate_tenant_graph_size = AsyncMock(return_value=100)

        with pytest.raises(GraphTooLargeError, match="exceeding projection limit"):
            await mgr.ensure_projection("tenant-large")

    @pytest.mark.asyncio
    async def test_projection_allowed_when_within_limit(
        self, mock_driver: MagicMock, mock_redis: AsyncMock,
    ) -> None:
        guard_cfg = ProjectionGuardConfig(max_projection_nodes=500)
        mgr = GDSProjectionManager(
            driver=mock_driver,
            redis_conn=mock_redis,
            guard_config=guard_cfg,
        )
        mgr._estimate_tenant_graph_size = AsyncMock(return_value=100)

        result = await mgr.ensure_projection("tenant-small")
        assert result is True

    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrent_projections(
        self, mock_driver: MagicMock, mock_redis: AsyncMock,
    ) -> None:
        guard_cfg = ProjectionGuardConfig(
            max_projection_nodes=10_000,
            max_concurrent_projections=1,
        )
        mgr = GDSProjectionManager(
            driver=mock_driver,
            redis_conn=mock_redis,
            guard_config=guard_cfg,
        )
        mgr._estimate_tenant_graph_size = AsyncMock(return_value=100)

        barrier = asyncio.Event()
        original_project = mgr._project_graph

        call_count = 0

        async def slow_project(tenant_id: str) -> None:
            nonlocal call_count
            call_count += 1
            await barrier.wait()

        mgr._project_graph = slow_project

        task1 = asyncio.create_task(mgr.ensure_projection("t1"))
        await asyncio.sleep(0.05)

        task2_started = asyncio.Event()

        async def monitored_ensure() -> bool:
            task2_started.set()
            return await mgr.ensure_projection("t2")

        task2 = asyncio.create_task(monitored_ensure())
        await asyncio.sleep(0.05)

        assert call_count == 1

        barrier.set()
        await task1
        await task2
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_guard_disabled_when_no_config(
        self, mock_driver: MagicMock, mock_redis: AsyncMock,
    ) -> None:
        mgr = GDSProjectionManager(
            driver=mock_driver,
            redis_conn=mock_redis,
        )
        assert mgr._guard is not None
        assert mgr._guard.max_projection_nodes == 500_000
