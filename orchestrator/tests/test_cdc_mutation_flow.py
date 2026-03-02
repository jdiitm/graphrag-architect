from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.mutation_publisher import (
    GraphMutationEvent,
    KafkaMutationPublisher,
)
from orchestrator.app.vector_sync_consumer import VectorSyncKafkaConsumer


class TestConsumerHandlesUpsertMutations:

    @pytest.mark.asyncio
    async def test_node_upsert_calls_vector_upserter(self) -> None:
        deleter = AsyncMock()
        upserter = AsyncMock()
        consumer = VectorSyncKafkaConsumer(
            transport=MagicMock(events=[]),
            vector_deleter=deleter,
            vector_upserter=upserter,
        )
        event = GraphMutationEvent(
            mutation_type="node_upsert",
            entity_ids=["svc-a", "svc-b"],
            tenant_id="t1",
        )
        await consumer.process_event(event)

        upserter.upsert.assert_awaited_once()
        call_args = upserter.upsert.call_args
        assert call_args.kwargs.get("entity_ids") == ["svc-a", "svc-b"]
        assert call_args.kwargs.get("tenant_id") == "t1"
        deleter.delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_edge_upsert_calls_vector_upserter(self) -> None:
        deleter = AsyncMock()
        upserter = AsyncMock()
        consumer = VectorSyncKafkaConsumer(
            transport=MagicMock(events=[]),
            vector_deleter=deleter,
            vector_upserter=upserter,
        )
        event = GraphMutationEvent(
            mutation_type="edge_upsert",
            entity_ids=["edge-1"],
            tenant_id="t1",
        )
        await consumer.process_event(event)

        upserter.upsert.assert_awaited_once()
        deleter.delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delete_still_calls_deleter(self) -> None:
        deleter = AsyncMock()
        upserter = AsyncMock()
        consumer = VectorSyncKafkaConsumer(
            transport=MagicMock(events=[]),
            vector_deleter=deleter,
            vector_upserter=upserter,
        )
        event = GraphMutationEvent(
            mutation_type="node_delete",
            entity_ids=["svc-a"],
            tenant_id="t1",
        )
        await consumer.process_event(event)

        deleter.delete.assert_awaited_once_with(["svc-a"])
        upserter.upsert.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_consumer_without_upserter_ignores_upserts(self) -> None:
        deleter = AsyncMock()
        consumer = VectorSyncKafkaConsumer(
            transport=MagicMock(events=[]),
            vector_deleter=deleter,
        )
        event = GraphMutationEvent(
            mutation_type="node_upsert",
            entity_ids=["svc-a"],
            tenant_id="t1",
        )
        await consumer.process_event(event)

        deleter.delete.assert_not_awaited()
        assert consumer.processed_count == 1


class TestGraphBuilderPublishesMutations:

    @pytest.mark.asyncio
    async def test_post_commit_publishes_node_upsert_events(self) -> None:
        from orchestrator.app.graph_builder import _post_commit_side_effects

        mock_repo = AsyncMock()
        mock_repo.prune_stale_edges = AsyncMock(return_value=(0, []))
        mock_span = MagicMock()
        mock_publisher = AsyncMock()

        committed_ids = {"svc-a", "svc-b"}
        with patch(
            "orchestrator.app.graph_builder._get_mutation_publisher",
            return_value=mock_publisher,
        ), patch(
            "orchestrator.app.graph_builder.invalidate_caches_after_ingest",
            new_callable=AsyncMock,
        ):
            await _post_commit_side_effects(
                repo=mock_repo,
                ingestion_id="ing-1",
                tenant_id="t1",
                span=mock_span,
                committed_node_ids=committed_ids,
            )

        mock_publisher.publish.assert_awaited_once()
        events = mock_publisher.publish.call_args[0][0]
        assert len(events) == 1
        assert events[0].mutation_type == "node_upsert"
        assert set(events[0].entity_ids) == {"svc-a", "svc-b"}
        assert events[0].tenant_id == "t1"

    @pytest.mark.asyncio
    async def test_post_commit_publishes_edge_tombstone_on_prune(self) -> None:
        from orchestrator.app.graph_builder import _post_commit_side_effects

        mock_repo = AsyncMock()
        mock_repo.prune_stale_edges = AsyncMock(
            return_value=(2, ["edge-1", "edge-2"]),
        )
        mock_span = MagicMock()
        mock_publisher = AsyncMock()

        with patch(
            "orchestrator.app.graph_builder._get_mutation_publisher",
            return_value=mock_publisher,
        ), patch(
            "orchestrator.app.graph_builder._enqueue_vector_cleanup",
            new_callable=AsyncMock,
        ), patch(
            "orchestrator.app.graph_builder.invalidate_caches_after_ingest",
            new_callable=AsyncMock,
        ), patch(
            "orchestrator.app.graph_builder._safe_drain_vector_outbox",
            new_callable=AsyncMock,
        ):
            await _post_commit_side_effects(
                repo=mock_repo,
                ingestion_id="ing-1",
                tenant_id="t1",
                span=mock_span,
                committed_node_ids={"svc-a"},
            )

        mock_publisher.publish.assert_awaited()
        all_events: List[GraphMutationEvent] = []
        for call in mock_publisher.publish.call_args_list:
            all_events.extend(call[0][0])
        tombstone_events = [
            e for e in all_events
            if e.mutation_type == "edge_tombstone"
        ]
        assert len(tombstone_events) >= 1
        assert set(tombstone_events[0].entity_ids) == {"edge-1", "edge-2"}


    @pytest.mark.asyncio
    async def test_post_commit_publishes_both_upsert_and_tombstone(self) -> None:
        from orchestrator.app.graph_builder import _post_commit_side_effects

        mock_repo = AsyncMock()
        mock_repo.prune_stale_edges = AsyncMock(
            return_value=(1, ["edge-1"]),
        )
        mock_span = MagicMock()
        mock_publisher = AsyncMock()

        with patch(
            "orchestrator.app.graph_builder._get_mutation_publisher",
            return_value=mock_publisher,
        ), patch(
            "orchestrator.app.graph_builder._enqueue_vector_cleanup",
            new_callable=AsyncMock,
        ), patch(
            "orchestrator.app.graph_builder.invalidate_caches_after_ingest",
            new_callable=AsyncMock,
        ), patch(
            "orchestrator.app.graph_builder._safe_drain_vector_outbox",
            new_callable=AsyncMock,
        ):
            await _post_commit_side_effects(
                repo=mock_repo,
                ingestion_id="ing-1",
                tenant_id="t1",
                span=mock_span,
                committed_node_ids={"svc-a"},
            )

        mock_publisher.publish.assert_awaited()
        all_events: List[GraphMutationEvent] = []
        for call in mock_publisher.publish.call_args_list:
            all_events.extend(call[0][0])
        types = {e.mutation_type for e in all_events}
        assert "node_upsert" in types
        assert "edge_tombstone" in types


class TestMutationPublisherWiring:

    def test_get_mutation_publisher_returns_none_without_kafka(self) -> None:
        from orchestrator.app.graph_builder import (
            _MutationPublisherHolder,
            _get_mutation_publisher,
        )
        _MutationPublisherHolder.value = None
        with patch.dict("os.environ", {}, clear=False):
            os_env = dict(**__import__("os").environ)
            os_env.pop("KAFKA_BROKERS", None)
            with patch.dict("os.environ", os_env, clear=True):
                publisher = _get_mutation_publisher()
                assert publisher is None

    def test_get_mutation_publisher_returns_publisher_with_kafka(self) -> None:
        from orchestrator.app.graph_builder import (
            _MutationPublisherHolder,
            _get_mutation_publisher,
        )
        _MutationPublisherHolder.value = None
        with patch.dict(
            "os.environ",
            {"KAFKA_BROKERS": "localhost:9092"},
            clear=False,
        ):
            publisher = _get_mutation_publisher()
            assert publisher is not None
            _MutationPublisherHolder.value = None

    def test_mutation_publisher_is_cached(self) -> None:
        from orchestrator.app.graph_builder import (
            _MutationPublisherHolder,
            _get_mutation_publisher,
        )
        _MutationPublisherHolder.value = None
        with patch.dict(
            "os.environ",
            {"KAFKA_BROKERS": "localhost:9092"},
            clear=False,
        ):
            pub1 = _get_mutation_publisher()
            pub2 = _get_mutation_publisher()
            assert pub1 is pub2
            _MutationPublisherHolder.value = None


class TestEndToEndCDCFlow:

    @pytest.mark.asyncio
    async def test_publish_then_consume_upsert(self) -> None:
        published: List[Dict[str, Any]] = []

        class InMemoryTransport:
            async def publish(
                self, topic: str, events: List[Dict[str, Any]],
            ) -> None:
                published.extend(events)

        publisher = KafkaMutationPublisher(
            transport=InMemoryTransport(),
        )
        events = [
            GraphMutationEvent(
                mutation_type="node_upsert",
                entity_ids=["svc-a"],
                tenant_id="t1",
            ),
        ]
        await publisher.publish(events)
        assert len(published) == 1

        upserter = AsyncMock()
        deleter = AsyncMock()
        consumer = VectorSyncKafkaConsumer(
            transport=MagicMock(events=[]),
            vector_deleter=deleter,
            vector_upserter=upserter,
        )
        await consumer.process_raw(published[0])

        upserter.upsert.assert_awaited_once()
        deleter.delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_publish_then_consume_delete(self) -> None:
        published: List[Dict[str, Any]] = []

        class InMemoryTransport:
            async def publish(
                self, topic: str, events: List[Dict[str, Any]],
            ) -> None:
                published.extend(events)

        publisher = KafkaMutationPublisher(
            transport=InMemoryTransport(),
        )
        events = [
            GraphMutationEvent(
                mutation_type="node_delete",
                entity_ids=["svc-a"],
                tenant_id="t1",
            ),
        ]
        await publisher.publish(events)

        upserter = AsyncMock()
        deleter = AsyncMock()
        consumer = VectorSyncKafkaConsumer(
            transport=MagicMock(events=[]),
            vector_deleter=deleter,
            vector_upserter=upserter,
        )
        await consumer.process_raw(published[0])

        deleter.delete.assert_awaited_once_with(["svc-a"])
        upserter.upsert.assert_not_awaited()
