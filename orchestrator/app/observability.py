from __future__ import annotations

import os
from typing import TYPE_CHECKING, Optional, Sequence

from opentelemetry import context as otel_context
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
    SpanExporter,
)
from opentelemetry.sdk.trace.sampling import (
    Decision,
    Sampler,
    SamplingResult,
    TraceIdRatioBased,
)
from opentelemetry.trace import SpanKind, StatusCode

if TYPE_CHECKING:
    from opentelemetry.context import Context
    from opentelemetry.trace import Link
    from opentelemetry.util.types import Attributes

_SERVICE_NAME = "graphrag-orchestrator"
_STATE: dict[str, trace.Tracer] = {}

_SUPPRESS_KEY = otel_context.create_key("suppress-instrumentation")


class RecordAllSampler(Sampler):
    def __init__(self, ratio: float) -> None:
        self._delegate = TraceIdRatioBased(ratio)

    def should_sample(
        self,
        parent_context: Optional[Context],
        trace_id: int,
        name: str,
        kind: Optional[SpanKind] = None,
        attributes: Optional[Attributes] = None,
        links: Optional[Sequence[Link]] = None,
        trace_state: Optional["trace.TraceState"] = None,
    ) -> SamplingResult:
        result = self._delegate.should_sample(
            parent_context, trace_id, name, kind, attributes, links, trace_state
        )
        if result.decision == Decision.DROP:
            return SamplingResult(
                Decision.RECORD_ONLY,
                result.attributes,
                result.trace_state,
            )
        return result

    def get_description(self) -> str:
        return f"RecordAllSampler({self._delegate.get_description()})"


class ErrorForceExportProcessor(SimpleSpanProcessor):
    def __init__(self, span_exporter: SpanExporter) -> None:
        super().__init__(span_exporter)
        self._error_exporter = span_exporter

    def on_start(
        self, span: "ReadableSpan", parent_context: Optional["Context"] = None
    ) -> None:
        pass

    def on_end(self, span: ReadableSpan) -> None:
        if not span.status or span.status.status_code != StatusCode.ERROR:
            return
        if not span.context or span.context.trace_flags.sampled:
            return
        token = otel_context.attach(otel_context.set_value(_SUPPRESS_KEY, True))
        try:
            self._error_exporter.export((span,))
        except Exception:
            pass
        finally:
            otel_context.detach(token)

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True

    def shutdown(self) -> None:
        pass


def _build_sampler(ratio: Optional[float] = None) -> RecordAllSampler:
    if ratio is None:
        ratio = float(os.environ.get("OTEL_TRACES_SAMPLER_ARG", "0.1"))
    return RecordAllSampler(ratio)


def configure_telemetry(
    exporter: Optional[SpanExporter] = None,
) -> TracerProvider:
    if exporter is not None:
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    else:
        sampler = _build_sampler()
        provider = TracerProvider(sampler=sampler)
        otlp_endpoint = os.environ.get(
            "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
        )
        otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
        provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        provider.add_span_processor(ErrorForceExportProcessor(otlp_exporter))

    trace.set_tracer_provider(provider)
    _STATE["tracer"] = provider.get_tracer(_SERVICE_NAME)
    return provider


def get_tracer() -> trace.Tracer:
    tracer = _STATE.get("tracer")
    if tracer is None:
        return trace.get_tracer(_SERVICE_NAME)
    return tracer


def configure_metrics() -> MeterProvider:
    reader = PrometheusMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    metrics.set_meter_provider(provider)
    return provider


meter = metrics.get_meter(_SERVICE_NAME)

INGESTION_DURATION = meter.create_histogram(
    name="ingestion.duration_ms",
    description="Ingestion pipeline node duration in milliseconds",
)

LLM_EXTRACTION_DURATION = meter.create_histogram(
    name="llm.extraction_duration_ms",
    description="LLM extraction call duration in milliseconds",
)

NEO4J_TRANSACTION_DURATION = meter.create_histogram(
    name="neo4j.transaction_duration_ms",
    description="Neo4j transaction duration in milliseconds",
)

QUERY_DURATION = meter.create_histogram(
    name="query.duration_ms",
    description="Query pipeline node duration in milliseconds",
)

QUERY_TOTAL = meter.create_counter(
    name="query.total",
    description="Total query requests",
)

QUERY_ERRORS = meter.create_counter(
    name="query.errors",
    description="Total query errors (5xx)",
)

INGESTION_TOTAL = meter.create_counter(
    name="ingestion.total",
    description="Total ingestion requests",
)

INGESTION_ERRORS = meter.create_counter(
    name="ingestion.errors",
    description="Total ingestion errors (5xx)",
)

EMBEDDING_FALLBACK_TOTAL = meter.create_counter(
    name="embedding.fallback_total",
    description="Total embedding fallbacks to fulltext search",
)


class ErrorBudgetTracker:
    def __init__(self, slo_target: float = 0.995) -> None:
        self._slo_target = slo_target
        self._total = 0
        self._errors = 0

    @property
    def slo_target(self) -> float:
        return self._slo_target

    @property
    def total_requests(self) -> int:
        return self._total

    @property
    def error_count(self) -> int:
        return self._errors

    @property
    def error_rate(self) -> float:
        if self._total == 0:
            return 0.0
        return self._errors / self._total

    @property
    def budget_remaining(self) -> float:
        allowed_error_rate = 1.0 - self._slo_target
        return max(0.0, allowed_error_rate - self.error_rate)

    @property
    def budget_exhausted(self) -> bool:
        return self.budget_remaining <= 0.0

    def record_request(self, is_error: bool = False) -> None:
        self._total += 1
        if is_error:
            self._errors += 1

    def reset(self) -> None:
        self._total = 0
        self._errors = 0


_QUERY_BUDGET = ErrorBudgetTracker(slo_target=0.995)
_INGESTION_BUDGET = ErrorBudgetTracker(slo_target=0.99)


def get_query_error_budget() -> ErrorBudgetTracker:
    return _QUERY_BUDGET


def get_ingestion_error_budget() -> ErrorBudgetTracker:
    return _INGESTION_BUDGET
