from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class TracingPort(Protocol):
    def start_span(self, name: str) -> Any: ...


@runtime_checkable
class MetricsPort(Protocol):
    def record_duration(
        self,
        name: str,
        duration_ms: float,
        attributes: Dict[str, str],
    ) -> None: ...

    def increment_counter(
        self,
        name: str,
        amount: int = 1,
        attributes: Optional[Dict[str, str]] = None,
    ) -> None: ...


class _NoopSpan:
    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def set_status(self, status_code: Any, description: str = "") -> None:
        pass

    def record_exception(self, exception: BaseException) -> None:
        pass

    def __enter__(self) -> _NoopSpan:
        return self

    def __exit__(self, *args: Any) -> None:
        pass


class NoopTracingPort:
    def start_span(self, name: str) -> _NoopSpan:
        return _NoopSpan()


class NoopMetricsPort:
    def record_duration(
        self,
        name: str,
        duration_ms: float,
        attributes: Dict[str, str],
    ) -> None:
        pass

    def increment_counter(
        self,
        name: str,
        amount: int = 1,
        attributes: Optional[Dict[str, str]] = None,
    ) -> None:
        pass


class OTelTracingPort:
    def __init__(self, tracer: Any) -> None:
        self._tracer = tracer

    def start_span(self, name: str) -> Any:
        return self._tracer.start_as_current_span(name)


class OTelMetricsPort:
    def __init__(
        self,
        histograms: Dict[str, Any],
        counters: Dict[str, Any],
    ) -> None:
        self._histograms = histograms
        self._counters = counters

    def record_duration(
        self,
        name: str,
        duration_ms: float,
        attributes: Dict[str, str],
    ) -> None:
        hist = self._histograms.get(name)
        if hist is not None:
            hist.record(duration_ms, attributes)

    def increment_counter(
        self,
        name: str,
        amount: int = 1,
        attributes: Optional[Dict[str, str]] = None,
    ) -> None:
        counter = self._counters.get(name)
        if counter is not None:
            counter.add(amount, attributes or {})


_PORT_STATE: Dict[str, Any] = {
    "tracing": NoopTracingPort(),
    "metrics": NoopMetricsPort(),
}


def get_tracing_port() -> TracingPort:
    return _PORT_STATE["tracing"]


def set_tracing_port(port: TracingPort) -> None:
    _PORT_STATE["tracing"] = port


def get_metrics_port() -> MetricsPort:
    return _PORT_STATE["metrics"]


def set_metrics_port(port: MetricsPort) -> None:
    _PORT_STATE["metrics"] = port
