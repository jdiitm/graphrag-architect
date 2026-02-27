from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app import neo4j_pool
from orchestrator.app.tenant_isolation import (
    IsolationMode,
    TenantConfig,
    TenantRegistry,
)


@pytest.fixture(autouse=True)
def _reset_pool():
    neo4j_pool._state["driver"] = MagicMock()
    neo4j_pool._state["query_timeout"] = 30.0
    neo4j_pool._state["database"] = "neo4j"
    yield
    neo4j_pool._state["driver"] = None
    neo4j_pool._state["query_timeout"] = None
    neo4j_pool._state["database"] = None


class TestFailClosedTenantRouting:

    def test_unknown_tenant_raises_error(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(tenant_id="known"))

        with pytest.raises(neo4j_pool.UnknownTenantError):
            neo4j_pool.resolve_database_for_tenant(
                registry, "unknown-tenant",
            )

    def test_none_registry_raises_error(self) -> None:
        with pytest.raises(neo4j_pool.RegistryUnavailableError):
            neo4j_pool.resolve_database_for_tenant(None, "any-tenant")

    def test_registered_physical_tenant_returns_database(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(
            tenant_id="acme",
            isolation_mode=IsolationMode.PHYSICAL,
            database_name="acme_db",
        ))

        result = neo4j_pool.resolve_database_for_tenant(registry, "acme")
        assert result == "acme_db"

    def test_registered_logical_tenant_returns_default(self) -> None:
        registry = TenantRegistry()
        registry.register(TenantConfig(
            tenant_id="startup",
            isolation_mode=IsolationMode.LOGICAL,
            database_name="startup_db",
        ))

        result = neo4j_pool.resolve_database_for_tenant(
            registry, "startup",
        )
        assert result == "neo4j"

    def test_resolve_driver_empty_tenant_still_works(self) -> None:
        driver, database = neo4j_pool.resolve_driver_for_tenant(
            registry=None, tenant_id="",
        )
        assert driver is not None
        assert database == "neo4j"

    def test_resolve_driver_unknown_tenant_raises(self) -> None:
        registry = TenantRegistry()

        with pytest.raises(neo4j_pool.UnknownTenantError):
            neo4j_pool.resolve_driver_for_tenant(registry, "ghost")


class TestStreamingIngestionNotAccumulating:

    def test_load_files_sync_streams_not_accumulates(self) -> None:
        from orchestrator.app.graph_builder import _load_files_sync

        chunks_yielded: List[int] = []

        def mock_chunked_loader(
            directory_path: str, **kwargs: Any,
        ):
            for i in range(5):
                chunk = [{"path": f"file_{i}.py", "content": "x" * 100}]
                chunks_yielded.append(i)
                yield chunk

        with patch(
            "orchestrator.app.graph_builder.load_directory_chunked",
            side_effect=mock_chunked_loader,
        ):
            files, skipped = _load_files_sync("/fake/dir", max_bytes=10000)

        assert len(files) <= 50
        assert len(chunks_yielded) == 5


class TestConcurrentIngestionLock:

    @pytest.mark.asyncio
    async def test_concurrent_commits_are_serialized(self) -> None:
        from orchestrator.app.graph_builder import acquire_ingestion_lock

        lock_acquired_order: List[int] = []
        release_events: List[asyncio.Event] = [
            asyncio.Event(), asyncio.Event(),
        ]

        async def worker(worker_id: int) -> None:
            async with acquire_ingestion_lock("tenant-a", "namespace-1"):
                lock_acquired_order.append(worker_id)
                release_events[worker_id].set()
                if worker_id == 0:
                    await asyncio.sleep(0.05)

        task0 = asyncio.create_task(worker(0))
        await asyncio.sleep(0.01)
        task1 = asyncio.create_task(worker(1))
        await asyncio.gather(task0, task1)

        assert lock_acquired_order[0] == 0
        assert lock_acquired_order[1] == 1

    @pytest.mark.asyncio
    async def test_different_tenants_not_blocked(self) -> None:
        from orchestrator.app.graph_builder import acquire_ingestion_lock

        concurrent_count = 0
        max_concurrent = 0

        async def worker(tenant: str) -> None:
            nonlocal concurrent_count, max_concurrent
            async with acquire_ingestion_lock(tenant, "ns"):
                concurrent_count += 1
                max_concurrent = max(max_concurrent, concurrent_count)
                await asyncio.sleep(0.02)
                concurrent_count -= 1

        await asyncio.gather(worker("tenant-a"), worker("tenant-b"))
        assert max_concurrent == 2


class TestSupernodeDegreeProperty:

    def test_single_hop_query_uses_degree_property(self) -> None:
        from orchestrator.app.query_engine import _build_single_hop_cypher

        cypher = _build_single_hop_cypher()
        assert "m.degree" in cypher
        assert "size((m)--())" not in cypher


class TestVectorDrainIsAsync:

    @pytest.mark.asyncio
    async def test_post_commit_does_not_block_on_drain(self) -> None:
        from orchestrator.app.graph_builder import _post_commit_side_effects

        drain_started = asyncio.Event()
        drain_complete = asyncio.Event()

        async def slow_drain() -> int:
            drain_started.set()
            await asyncio.sleep(0.5)
            drain_complete.set()
            return 0

        mock_repo = MagicMock()
        mock_repo.prune_stale_edges = AsyncMock(return_value=(0, []))
        mock_span = MagicMock()

        with patch(
            "orchestrator.app.graph_builder.drain_vector_outbox",
            side_effect=slow_drain,
        ), patch(
            "orchestrator.app.graph_builder.invalidate_caches_after_ingest",
            new_callable=AsyncMock,
        ):
            start = asyncio.get_event_loop().time()
            await _post_commit_side_effects(
                mock_repo, "ing-123", "tenant-1", mock_span,
            )
            elapsed = asyncio.get_event_loop().time() - start

        assert elapsed < 0.4
