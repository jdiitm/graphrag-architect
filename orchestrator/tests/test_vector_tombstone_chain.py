from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.extraction_models import CallsEdge, ServiceNode


_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}

_REDIS_ENV = {**_ENV_VARS, "REDIS_URL": "redis://localhost:6379"}


def _make_services(count: int) -> List[ServiceNode]:
    return [
        ServiceNode(
            id=f"svc-{i}", name=f"service-{i}", language="go",
            framework="gin", opentelemetry_enabled=False, confidence=1.0,
            tenant_id="test-tenant",
        )
        for i in range(count)
    ]


class TestTombstoneReturnsNodeIds:

    @pytest.mark.asyncio
    async def test_tombstone_stale_edges_returns_affected_ids(self) -> None:
        from orchestrator.app.neo4j_client import GraphRepository

        mock_session = AsyncMock()

        async def _fake_write(fn, **kwargs):
            return 3, ["svc-old-1", "svc-old-2", "svc-old-3"]

        mock_session.execute_write = _fake_write

        mock_driver = MagicMock()

        @asynccontextmanager
        async def session_ctx(**kwargs):
            yield mock_session

        mock_driver.session = session_ctx

        repo = GraphRepository(mock_driver)
        count, ids = await repo.tombstone_stale_edges("run-new")

        assert count >= 0
        assert isinstance(ids, list)

    @pytest.mark.asyncio
    async def test_prune_stale_edges_returns_tuple(self) -> None:
        from orchestrator.app.neo4j_client import GraphRepository

        mock_session = AsyncMock()

        async def _fake_write(fn, **kwargs):
            return 0, []

        mock_session.execute_write = _fake_write

        mock_driver = MagicMock()

        @asynccontextmanager
        async def session_ctx(**kwargs):
            yield mock_session

        mock_driver.session = session_ctx

        repo = GraphRepository(mock_driver)
        result = await repo.prune_stale_edges("run-123")
        assert isinstance(result, tuple)
        assert len(result) == 2
        count, ids = result
        assert isinstance(count, int)
        assert isinstance(ids, list)


class TestVectorStoreCleanupOnPrune:

    @pytest.mark.asyncio
    async def test_commit_calls_vector_delete_after_prune(self) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        vector_delete_calls: List[tuple] = []

        async def _tracking_commit(self_repo, entities):
            pass

        async def _tracking_prune(self_repo, current_ingestion_id="", max_age_hours=24):
            return 2, ["svc-stale-1", "svc-stale-2"]

        async def _tracking_vector_delete(collection, ids):
            vector_delete_calls.append((collection, ids))
            return len(ids)

        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()
        entities = _make_services(3)

        mock_vector_store = AsyncMock()
        mock_vector_store.delete = _tracking_vector_delete

        from orchestrator.app import graph_builder as _gb
        from orchestrator.app.vector_sync_outbox import OutboxDrainer

        original = _gb._post_commit_side_effects

        async def _force_inmemory(*args: Any, **kwargs: Any) -> None:
            kwargs["neo4j_driver"] = None
            return await original(*args, **kwargs)

        async def _drain_inmemory() -> int:
            drainer = OutboxDrainer(
                outbox=_gb._VECTOR_OUTBOX, vector_store=mock_vector_store,
            )
            return await drainer.process_once()

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.get_driver",
                return_value=mock_driver,
            ),
            patch(
                "orchestrator.app.neo4j_client.GraphRepository.commit_topology",
                _tracking_commit,
            ),
            patch(
                "orchestrator.app.neo4j_client.GraphRepository.prune_stale_edges",
                _tracking_prune,
            ),
            patch(
                "orchestrator.app.graph_builder.get_vector_store",
                return_value=mock_vector_store,
            ),
            patch.object(
                _gb, "_post_commit_side_effects",
                side_effect=_force_inmemory,
            ),
            patch.object(
                _gb, "drain_vector_outbox",
                side_effect=_drain_inmemory,
            ),
        ):
            result = await commit_to_neo4j({"extracted_nodes": entities})
            from orchestrator.app.graph_builder import _BACKGROUND_TASKS
            if _BACKGROUND_TASKS:
                await asyncio.gather(*_BACKGROUND_TASKS)

        assert result["commit_status"] == "success"
        assert len(vector_delete_calls) >= 1, (
            "commit_to_neo4j must call VectorStore.delete after prune_stale_edges "
            "returns tombstoned node IDs"
        )
        deleted_ids = vector_delete_calls[0][1]
        assert "svc-stale-1" in deleted_ids
        assert "svc-stale-2" in deleted_ids

    @pytest.mark.asyncio
    async def test_commit_skips_vector_delete_when_no_pruned_ids(self) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        vector_delete_calls: List[tuple] = []

        async def _tracking_commit(self_repo, entities):
            pass

        async def _tracking_prune(self_repo, current_ingestion_id="", max_age_hours=24):
            return 0, []

        async def _tracking_vector_delete(collection, ids):
            vector_delete_calls.append((collection, ids))
            return 0

        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()
        entities = _make_services(1)

        mock_vector_store = AsyncMock()
        mock_vector_store.delete = _tracking_vector_delete

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.get_driver",
                return_value=mock_driver,
            ),
            patch(
                "orchestrator.app.neo4j_client.GraphRepository.commit_topology",
                _tracking_commit,
            ),
            patch(
                "orchestrator.app.neo4j_client.GraphRepository.prune_stale_edges",
                _tracking_prune,
            ),
            patch(
                "orchestrator.app.graph_builder.get_vector_store",
                return_value=mock_vector_store,
            ),
        ):
            result = await commit_to_neo4j({"extracted_nodes": entities})

        assert result["commit_status"] == "success"
        assert len(vector_delete_calls) == 0, (
            "commit_to_neo4j must NOT call VectorStore.delete when no IDs were pruned"
        )


class TestDurableOutboxBranch:

    @pytest.mark.asyncio
    async def test_post_commit_uses_durable_outbox_when_driver_present(self) -> None:
        from orchestrator.app.graph_builder import _post_commit_side_effects
        from orchestrator.app.neo4j_client import GraphRepository

        mock_session = AsyncMock()

        async def _fake_write(fn, **kwargs):
            return 2, ["svc-1", "svc-2"]

        mock_session.execute_write = _fake_write

        mock_driver = MagicMock()

        @asynccontextmanager
        async def session_ctx(**kwargs):
            yield mock_session

        mock_driver.session = session_ctx

        repo = GraphRepository(mock_driver)
        span = MagicMock()

        write_event_calls: List[Any] = []
        original_write_event = AsyncMock(
            side_effect=lambda event: write_event_calls.append(event),
        )

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.Neo4jOutboxStore.write_event",
                original_write_event,
            ),
            patch(
                "orchestrator.app.graph_builder.get_vector_store",
                return_value=AsyncMock(),
            ),
            patch(
                "orchestrator.app.graph_builder._safe_drain_vector_outbox",
                new_callable=AsyncMock,
            ),
        ):
            await _post_commit_side_effects(
                repo=repo,
                ingestion_id="run-123",
                tenant_id="test-tenant",
                span=span,
                neo4j_driver=mock_driver,
            )

        original_write_event.assert_called_once()
        event = original_write_event.call_args[0][0]
        assert "svc-1" in event.pruned_ids
        assert "svc-2" in event.pruned_ids

    @pytest.mark.asyncio
    async def test_post_commit_falls_back_to_inmemory_without_driver(self) -> None:
        from orchestrator.app.graph_builder import (
            _VECTOR_OUTBOX,
            _post_commit_side_effects,
        )
        from orchestrator.app.neo4j_client import GraphRepository

        mock_session = AsyncMock()

        async def _fake_write(fn, **kwargs):
            return 1, ["svc-orphan"]

        mock_session.execute_write = _fake_write

        mock_driver = MagicMock()

        @asynccontextmanager
        async def session_ctx(**kwargs):
            yield mock_session

        mock_driver.session = session_ctx

        repo = GraphRepository(mock_driver)
        span = MagicMock()

        before_count = _VECTOR_OUTBOX.pending_count

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.get_vector_store",
                return_value=AsyncMock(),
            ),
            patch(
                "orchestrator.app.graph_builder._safe_drain_vector_outbox",
                new_callable=AsyncMock,
            ),
        ):
            await _post_commit_side_effects(
                repo=repo,
                ingestion_id="run-456",
                tenant_id="test-tenant",
                span=span,
                neo4j_driver=None,
            )

        assert _VECTOR_OUTBOX.pending_count > before_count


class TestDrainVectorOutboxRedisReuse:

    @pytest.mark.asyncio
    async def test_drain_reuses_redis_connection_across_calls(self) -> None:
        from orchestrator.app.graph_builder import (
            _RedisHolder,
            drain_vector_outbox,
        )

        _RedisHolder.value = None

        mock_redis = AsyncMock()
        create_redis_calls: List[int] = []

        def _tracking_create(*args, **kwargs):
            create_redis_calls.append(1)
            return mock_redis

        mock_drainer = AsyncMock()
        mock_drainer.process_once = AsyncMock(return_value=0)

        with (
            patch.dict("os.environ", _REDIS_ENV),
            patch(
                "orchestrator.app.redis_client.create_async_redis",
                side_effect=_tracking_create,
            ),
            patch(
                "orchestrator.app.graph_builder.get_vector_store",
                return_value=AsyncMock(),
            ),
            patch(
                "orchestrator.app.graph_builder.create_outbox_drainer",
                return_value=mock_drainer,
            ),
        ):
            await drain_vector_outbox()
            await drain_vector_outbox()

        _RedisHolder.value = None

        assert len(create_redis_calls) == 1, (
            "drain_vector_outbox must reuse a cached Redis connection, "
            f"but create_async_redis was called {len(create_redis_calls)} time(s)"
        )
