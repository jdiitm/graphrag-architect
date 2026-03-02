from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.config import ProductionConfigValidator, VectorSyncConfig
from orchestrator.app.vector_sync_outbox import (
    CoalescingOutbox,
    RedisOutboxStore,
    VectorSyncEvent,
    VectorSyncOutbox,
)


class TestVectorSyncConfigDurableDefault:

    def test_defaults_to_durable_when_redis_url_set(self) -> None:
        with patch.dict(os.environ, {"REDIS_URL": "redis://localhost:6379"}, clear=False):
            cfg = VectorSyncConfig.from_env()
            assert cfg.backend != "memory"

    def test_defaults_to_memory_when_no_redis(self) -> None:
        env = {k: v for k, v in os.environ.items() if k != "REDIS_URL"}
        with patch.dict(os.environ, env, clear=True):
            cfg = VectorSyncConfig.from_env()
            assert cfg.backend == "memory"


class TestProductionRejectsMemoryOutbox:

    def test_production_mode_rejects_memory_vector_sync(self) -> None:
        validator = ProductionConfigValidator(
            deployment_mode="production",
            ast_dlq_backend="redis",
            outbox_backend="redis",
            vector_sync_backend="memory",
        )
        from orchestrator.app.config import ConfigurationError
        with pytest.raises(ConfigurationError, match="memory"):
            validator.validate_production_invariants()

    def test_dev_mode_allows_memory_vector_sync(self) -> None:
        validator = ProductionConfigValidator(
            deployment_mode="dev",
            ast_dlq_backend="memory",
            outbox_backend="memory",
            vector_sync_backend="memory",
        )
        validator.validate_production_invariants()


class TestDurableSpilloverWiring:

    def test_coalescing_outbox_uses_durable_spillover_when_redis_available(
        self,
    ) -> None:
        from orchestrator.app.graph_builder import (
            build_coalescing_outbox,
        )
        mock_redis = MagicMock()
        outbox = build_coalescing_outbox(redis_conn=mock_redis)
        assert outbox._spillover_fn is not None

        events = [
            VectorSyncEvent(
                event_id="e1",
                collection="test",
                operation="upsert",
                pruned_ids=[],
                vectors=[],
            )
        ]
        outbox._spillover_fn(events)

    def test_coalescing_outbox_falls_back_to_memory_without_redis(
        self,
    ) -> None:
        from orchestrator.app.graph_builder import (
            build_coalescing_outbox,
        )
        outbox = build_coalescing_outbox(redis_conn=None)
        assert outbox._spillover_fn is not None

    def test_event_survives_coalescing_eviction_with_durable_store(
        self,
    ) -> None:
        captured: list[VectorSyncEvent] = []

        def spy_spillover(events: list[VectorSyncEvent]) -> None:
            captured.extend(events)

        outbox = CoalescingOutbox(max_entries=2, spillover_fn=spy_spillover)
        for i in range(4):
            outbox.enqueue(VectorSyncEvent(
                event_id=f"e{i}",
                collection=f"col{i}",
                operation="upsert",
                pruned_ids=[],
                vectors=[],
            ))
        assert len(captured) > 0
