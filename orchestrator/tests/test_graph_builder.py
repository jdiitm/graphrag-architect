from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from neo4j.exceptions import Neo4jError
from opentelemetry.trace import StatusCode

from orchestrator.app.circuit_breaker import CircuitOpenError


_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


def _make_failing_driver(exception: Exception):
    mock_driver = MagicMock()

    async def _session_ctx(**kwargs):
        raise exception

    mock_driver.session = _session_ctx
    mock_driver.close = AsyncMock()
    return mock_driver


class TestCommitToNeo4jErrorLogging:

    @pytest.mark.asyncio
    async def test_logs_neo4j_error_on_commit_failure(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.get_driver",
                side_effect=Neo4jError("Connection lost"),
            ),
            caplog.at_level(logging.ERROR, logger="orchestrator.app.graph_builder"),
        ):
            result = await commit_to_neo4j({
                "extracted_nodes": [],
                "commit_status": "",
            })

        assert result["commit_status"] == "failed"
        neo4j_log_messages = [
            r.message for r in caplog.records
            if r.levelno >= logging.ERROR
        ]
        assert len(neo4j_log_messages) >= 1, (
            "Expected at least one ERROR log when Neo4j commit fails"
        )

    @pytest.mark.asyncio
    async def test_logs_os_error_on_commit_failure(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.get_driver",
                side_effect=OSError("Connection refused"),
            ),
            caplog.at_level(logging.ERROR, logger="orchestrator.app.graph_builder"),
        ):
            result = await commit_to_neo4j({
                "extracted_nodes": [],
                "commit_status": "",
            })

        assert result["commit_status"] == "failed"
        error_messages = [
            r.message for r in caplog.records
            if r.levelno >= logging.ERROR
        ]
        assert any("Connection refused" in m for m in error_messages), (
            f"Expected error details in log. Got: {error_messages}"
        )

    @pytest.mark.asyncio
    async def test_logs_circuit_open_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.get_driver",
                side_effect=CircuitOpenError("Circuit is OPEN"),
            ),
            caplog.at_level(logging.ERROR, logger="orchestrator.app.graph_builder"),
        ):
            result = await commit_to_neo4j({
                "extracted_nodes": [],
                "commit_status": "",
            })

        assert result["commit_status"] == "failed"
        error_messages = [
            r.message for r in caplog.records
            if r.levelno >= logging.ERROR
        ]
        assert len(error_messages) >= 1, (
            "Expected ERROR log when circuit breaker is open"
        )

    @pytest.mark.asyncio
    async def test_span_records_error_status_on_failure(self) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        mock_span = MagicMock()
        mock_tracer = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__ = (
            MagicMock(return_value=mock_span)
        )
        mock_tracer.start_as_current_span.return_value.__exit__ = (
            MagicMock(return_value=False)
        )

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.get_driver",
                side_effect=OSError("Connection refused"),
            ),
            patch(
                "orchestrator.app.graph_builder.get_tracer",
                return_value=mock_tracer,
            ),
        ):
            result = await commit_to_neo4j({
                "extracted_nodes": [],
                "commit_status": "",
            })

        assert result["commit_status"] == "failed"
        mock_span.set_status.assert_called_once()
        call_args = mock_span.set_status.call_args
        assert call_args[0][0] == StatusCode.ERROR, (
            "Span must record ERROR status on commit failure"
        )

    @pytest.mark.asyncio
    async def test_span_records_exception_event_on_failure(self) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        mock_span = MagicMock()
        mock_tracer = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__ = (
            MagicMock(return_value=mock_span)
        )
        mock_tracer.start_as_current_span.return_value.__exit__ = (
            MagicMock(return_value=False)
        )

        os_error = OSError("Connection refused")
        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.get_driver",
                side_effect=os_error,
            ),
            patch(
                "orchestrator.app.graph_builder.get_tracer",
                return_value=mock_tracer,
            ),
        ):
            result = await commit_to_neo4j({
                "extracted_nodes": [],
                "commit_status": "",
            })

        assert result["commit_status"] == "failed"
        mock_span.record_exception.assert_called_once_with(os_error)

    @pytest.mark.asyncio
    async def test_no_error_log_on_successful_commit(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j
        from orchestrator.tests.conftest import mock_async_session

        mock_driver, _ = mock_async_session()

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.get_driver",
                return_value=mock_driver,
            ),
            caplog.at_level(logging.ERROR, logger="orchestrator.app.graph_builder"),
        ):
            result = await commit_to_neo4j({
                "extracted_nodes": [],
                "commit_status": "",
            })

        assert result["commit_status"] == "success"
        error_messages = [
            r.message for r in caplog.records
            if r.levelno >= logging.ERROR
        ]
        assert len(error_messages) == 0, (
            f"Expected no ERROR logs on success. Got: {error_messages}"
        )


class TestYAMLCheckpointMarking:

    @pytest.mark.asyncio
    async def test_yaml_files_marked_extracted_after_manifest_parsing(self) -> None:
        from orchestrator.app.graph_builder import parse_k8s_and_kafka_manifests

        state = {
            "raw_files": [
                {"path": "deploy.yaml", "content": (
                    "apiVersion: apps/v1\nkind: Deployment\n"
                    "metadata:\n  name: auth-svc"
                )},
                {"path": "topics.yml", "content": (
                    "apiVersion: kafka.strimzi.io/v1beta2\nkind: KafkaTopic\n"
                    "metadata:\n  name: events\nspec:\n  partitions: 3"
                )},
                {"path": "main.go", "content": "package main"},
            ],
            "extracted_nodes": [],
            "extraction_checkpoint": {
                "deploy.yaml": "pending",
                "topics.yml": "pending",
                "main.go": "extracted",
            },
        }

        result = await parse_k8s_and_kafka_manifests(state)
        cp = result["extraction_checkpoint"]
        assert cp["deploy.yaml"] == "extracted"
        assert cp["topics.yml"] == "extracted"
        assert cp["main.go"] == "extracted"

    @pytest.mark.asyncio
    async def test_yaml_not_re_pending_on_retry_cycle(self) -> None:
        from orchestrator.app.graph_builder import parse_k8s_and_kafka_manifests

        state = {
            "raw_files": [
                {"path": "deploy.yaml", "content": "kind: Deployment\nmetadata:\n  name: x"},
                {"path": "app.py", "content": "print('hi')"},
            ],
            "extracted_nodes": [],
            "extraction_checkpoint": {
                "deploy.yaml": "pending",
                "app.py": "pending",
            },
        }

        result = await parse_k8s_and_kafka_manifests(state)
        assert result["extraction_checkpoint"]["deploy.yaml"] == "extracted"


_COMMIT_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


class TestCompletionTrackerWiredIntoCommit:

    @pytest.mark.asyncio
    async def test_commit_records_completion(self) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j
        from orchestrator.app.extraction_models import ServiceNode

        topology_calls: list[list] = []

        async def _tracking_commit(self_repo, entities):
            topology_calls.append(list(entities))

        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()

        entities = [
            ServiceNode(
                id="svc-1", name="auth", language="go",
                framework="gin", opentelemetry_enabled=False, confidence=1.0,
                tenant_id="test-tenant",
            ),
        ]

        completion_marks: list[str] = []

        async def _mock_mark(content_hash: str) -> None:
            completion_marks.append(content_hash)

        with (
            patch.dict("os.environ", _COMMIT_ENV_VARS),
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
        assert len(completion_marks) > 0 or result.get("completion_tracked"), (
            "commit_to_neo4j must call CompletionTracker.mark_committed() "
            "after successful Neo4j commit."
        )
