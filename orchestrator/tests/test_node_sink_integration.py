from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.extraction_models import CallsEdge, ServiceNode


_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


def _make_services(count: int) -> List[ServiceNode]:
    return [
        ServiceNode(
            id=f"svc-{i}",
            name=f"service-{i}",
            language="go",
            framework="gin",
            opentelemetry_enabled=False,
            confidence=1.0,
        )
        for i in range(count)
    ]


def _make_edges(count: int) -> List[CallsEdge]:
    return [
        CallsEdge(
            source_service_id=f"svc-{i}",
            target_service_id=f"svc-{i + 1}",
            protocol="grpc",
            confidence=1.0,
        )
        for i in range(count)
    ]


class TestCommitToNeo4jUsesIncrementalSink:

    @pytest.mark.asyncio
    async def test_large_entity_list_committed_via_sink_batches(self) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        topology_calls: List[List[Any]] = []
        original_commit = None

        async def _tracking_commit(self_repo, entities):
            topology_calls.append(list(entities))

        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()

        entities = _make_services(1200)

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
        ):
            result = await commit_to_neo4j({"extracted_nodes": entities})

        assert result["commit_status"] == "success"
        total_committed = sum(len(b) for b in topology_calls)
        assert total_committed == 1200
        assert len(topology_calls) > 1, (
            f"commit_topology called {len(topology_calls)} time(s) with sizes "
            f"{[len(b) for b in topology_calls]}. Expected multiple calls via "
            "IncrementalNodeSink, not a single call with all 1200 entities."
        )
        for batch in topology_calls:
            assert len(batch) <= 500, (
                f"Batch size {len(batch)} exceeds sink threshold of 500"
            )

    @pytest.mark.asyncio
    async def test_small_entity_list_still_committed(self) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        topology_calls: List[List[Any]] = []

        async def _tracking_commit(self_repo, entities):
            topology_calls.append(list(entities))

        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()
        entities = _make_services(3)

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
        ):
            result = await commit_to_neo4j({"extracted_nodes": entities})

        assert result["commit_status"] == "success"
        total_committed = sum(len(b) for b in topology_calls)
        assert total_committed == 3

    @pytest.mark.asyncio
    async def test_mixed_nodes_and_edges_all_committed(self) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        topology_calls: List[List[Any]] = []

        async def _tracking_commit(self_repo, entities):
            topology_calls.append(list(entities))

        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()
        entities: List[Any] = _make_services(5) + _make_edges(3)

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
        ):
            result = await commit_to_neo4j({"extracted_nodes": entities})

        assert result["commit_status"] == "success"
        total_committed = sum(len(b) for b in topology_calls)
        assert total_committed == 8

    @pytest.mark.asyncio
    async def test_empty_entity_list_succeeds(self) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        topology_calls: List[List[Any]] = []

        async def _tracking_commit(self_repo, entities):
            topology_calls.append(list(entities))

        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()

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
        ):
            result = await commit_to_neo4j({"extracted_nodes": []})

        assert result["commit_status"] == "success"
        assert len(topology_calls) == 0


class TestStreamingPipelineMemoryBound:

    @pytest.mark.asyncio
    async def test_streaming_does_not_accumulate_entities(self) -> None:
        from orchestrator.app.graph_builder import run_streaming_pipeline

        chunk_call_count = 0

        async def _fake_process_chunk(chunk):
            nonlocal chunk_call_count
            chunk_call_count += 1
            nodes = _make_services(50)
            return nodes, "success", {}

        state = {
            "directory_path": "/fake/path",
            "raw_files": [],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
            "extraction_checkpoint": {},
            "skipped_files": [],
        }

        fake_chunks = [
            [{"path": f"file_{i}.go", "content": "package main"}]
            for i in range(10)
        ]

        with (
            patch(
                "orchestrator.app.graph_builder.load_directory_chunked",
                return_value=iter(fake_chunks),
            ),
            patch(
                "orchestrator.app.graph_builder._process_chunk",
                side_effect=_fake_process_chunk,
            ),
            patch(
                "orchestrator.app.graph_builder._get_workspace_max_bytes",
                return_value=100_000_000,
            ),
        ):
            result = await run_streaming_pipeline(state)

        assert chunk_call_count == 10
        assert result["commit_status"] == "success"
        returned_entities = result.get("extracted_nodes", [])
        assert len(returned_entities) < 500, (
            f"Streaming pipeline returned {len(returned_entities)} entities. "
            "Expected bounded accumulation, not all 500 entities in memory."
        )
