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
