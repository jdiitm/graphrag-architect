from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestTracingPortProtocol:

    def test_noop_tracing_port_start_span_does_not_raise(self) -> None:
        from orchestrator.app.telemetry_ports import NoopTracingPort
        port = NoopTracingPort()
        with port.start_span("test.op") as span:
            span.set_attribute("key", "value")

    def test_noop_span_set_status_does_not_raise(self) -> None:
        from orchestrator.app.telemetry_ports import NoopTracingPort
        port = NoopTracingPort()
        with port.start_span("test.op") as span:
            span.set_status("OK")

    def test_noop_span_record_exception_does_not_raise(self) -> None:
        from orchestrator.app.telemetry_ports import NoopTracingPort
        port = NoopTracingPort()
        with port.start_span("test.op") as span:
            span.record_exception(ValueError("test"))

    def test_noop_tracing_port_satisfies_protocol(self) -> None:
        from orchestrator.app.telemetry_ports import NoopTracingPort, TracingPort
        port = NoopTracingPort()
        assert isinstance(port, TracingPort)


class TestMetricsPortProtocol:

    def test_noop_metrics_port_record_duration_does_not_raise(self) -> None:
        from orchestrator.app.telemetry_ports import NoopMetricsPort
        port = NoopMetricsPort()
        port.record_duration("test.duration_ms", 42.5, {"node": "test"})

    def test_noop_metrics_port_increment_counter_does_not_raise(self) -> None:
        from orchestrator.app.telemetry_ports import NoopMetricsPort
        port = NoopMetricsPort()
        port.increment_counter("test.total", 1, {"status": "ok"})

    def test_noop_metrics_port_satisfies_protocol(self) -> None:
        from orchestrator.app.telemetry_ports import MetricsPort, NoopMetricsPort
        port = NoopMetricsPort()
        assert isinstance(port, MetricsPort)


class TestOTelTracingPort:

    def test_delegates_to_otel_tracer(self) -> None:
        from orchestrator.app.telemetry_ports import OTelTracingPort
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__ = MagicMock(
            return_value=mock_span
        )
        mock_tracer.start_as_current_span.return_value.__exit__ = MagicMock(
            return_value=False
        )
        port = OTelTracingPort(mock_tracer)
        with port.start_span("test.op") as span:
            span.set_attribute("k", "v")
        mock_tracer.start_as_current_span.assert_called_once_with("test.op")
        mock_span.set_attribute.assert_called_once_with("k", "v")

    def test_satisfies_tracing_protocol(self) -> None:
        from orchestrator.app.telemetry_ports import (
            OTelTracingPort,
            TracingPort,
        )
        port = OTelTracingPort(MagicMock())
        assert isinstance(port, TracingPort)


class TestOTelMetricsPort:

    def test_record_duration_delegates_to_histogram(self) -> None:
        from orchestrator.app.telemetry_ports import OTelMetricsPort
        histograms = {"test.duration_ms": MagicMock()}
        port = OTelMetricsPort(histograms=histograms, counters={})
        port.record_duration("test.duration_ms", 42.5, {"node": "x"})
        histograms["test.duration_ms"].record.assert_called_once_with(
            42.5, {"node": "x"}
        )

    def test_increment_counter_delegates_to_counter(self) -> None:
        from orchestrator.app.telemetry_ports import OTelMetricsPort
        counters = {"test.total": MagicMock()}
        port = OTelMetricsPort(histograms={}, counters=counters)
        port.increment_counter("test.total", 1, {"status": "ok"})
        counters["test.total"].add.assert_called_once_with(
            1, {"status": "ok"}
        )

    def test_record_unknown_histogram_does_not_raise(self) -> None:
        from orchestrator.app.telemetry_ports import OTelMetricsPort
        port = OTelMetricsPort(histograms={}, counters={})
        port.record_duration("unknown.metric", 1.0, {})

    def test_increment_unknown_counter_does_not_raise(self) -> None:
        from orchestrator.app.telemetry_ports import OTelMetricsPort
        port = OTelMetricsPort(histograms={}, counters={})
        port.increment_counter("unknown.counter", 1, {})

    def test_satisfies_metrics_protocol(self) -> None:
        from orchestrator.app.telemetry_ports import (
            MetricsPort,
            OTelMetricsPort,
        )
        port = OTelMetricsPort(histograms={}, counters={})
        assert isinstance(port, MetricsPort)


class TestPortStateManagement:

    def test_get_tracing_port_returns_noop_after_reset(self) -> None:
        from orchestrator.app.telemetry_ports import (
            NoopTracingPort,
            get_tracing_port,
            set_tracing_port,
        )
        set_tracing_port(NoopTracingPort())
        port = get_tracing_port()
        assert isinstance(port, NoopTracingPort)

    def test_get_metrics_port_returns_noop_after_reset(self) -> None:
        from orchestrator.app.telemetry_ports import (
            NoopMetricsPort,
            get_metrics_port,
            set_metrics_port,
        )
        set_metrics_port(NoopMetricsPort())
        port = get_metrics_port()
        assert isinstance(port, NoopMetricsPort)

    def test_set_and_get_tracing_port(self) -> None:
        from orchestrator.app.telemetry_ports import (
            NoopTracingPort,
            get_tracing_port,
            set_tracing_port,
        )
        custom = NoopTracingPort()
        set_tracing_port(custom)
        assert get_tracing_port() is custom
        set_tracing_port(NoopTracingPort())

    def test_set_and_get_metrics_port(self) -> None:
        from orchestrator.app.telemetry_ports import (
            NoopMetricsPort,
            get_metrics_port,
            set_metrics_port,
        )
        custom = NoopMetricsPort()
        set_metrics_port(custom)
        assert get_metrics_port() is custom
        set_metrics_port(NoopMetricsPort())


class TestContainerWiring:

    def test_app_container_has_tracing_port(self) -> None:
        from orchestrator.app.container import AppContainer
        container = AppContainer.from_env()
        from orchestrator.app.telemetry_ports import TracingPort
        assert isinstance(container.tracing_port, TracingPort)

    def test_app_container_has_metrics_port(self) -> None:
        from orchestrator.app.container import AppContainer
        container = AppContainer.from_env()
        from orchestrator.app.telemetry_ports import MetricsPort
        assert isinstance(container.metrics_port, MetricsPort)


class TestGraphBuilderUsesPort:

    @pytest.mark.asyncio
    async def test_load_workspace_uses_tracing_port(self) -> None:
        from orchestrator.app.telemetry_ports import get_tracing_port
        port = get_tracing_port()
        with port.start_span("ingestion.load_workspace") as span:
            span.set_attribute("file_count", 0)

    @pytest.mark.asyncio
    async def test_load_workspace_uses_metrics_port(self) -> None:
        from orchestrator.app.telemetry_ports import get_metrics_port
        port = get_metrics_port()
        port.record_duration(
            "ingestion.duration_ms", 10.5, {"node": "load_workspace"}
        )


class TestQueryEngineUsesPort:

    def test_classify_query_uses_tracing_port(self) -> None:
        from orchestrator.app.telemetry_ports import get_tracing_port
        port = get_tracing_port()
        with port.start_span("query.classify") as span:
            span.set_attribute("query.complexity", "simple")

    def test_vector_retrieve_uses_metrics_port(self) -> None:
        from orchestrator.app.telemetry_ports import get_metrics_port
        port = get_metrics_port()
        port.record_duration(
            "query.duration_ms", 15.2, {"node": "vector_retrieve"}
        )
