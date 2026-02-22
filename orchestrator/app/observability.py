from __future__ import annotations

import os
from typing import Optional

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
    SpanExporter,
)
from opentelemetry.sdk.trace.sampling import ParentBasedTraceIdRatio
from opentelemetry.trace import StatusCode

_SERVICE_NAME = "graphrag-orchestrator"
_STATE: dict[str, trace.Tracer] = {}


class ErrorForceExportProcessor(SimpleSpanProcessor):
    def __init__(self, span_exporter: SpanExporter) -> None:
        super().__init__(span_exporter)
        self._error_exporter = span_exporter

    def on_end(self, span: "ReadableSpan") -> None:
        if span.status and span.status.status_code == StatusCode.ERROR:
            self._error_exporter.export((span,))

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True

    def shutdown(self) -> None:
        pass


def _build_sampler() -> ParentBasedTraceIdRatio:
    rate = float(os.environ.get("OTEL_TRACES_SAMPLER_ARG", "0.1"))
    return ParentBasedTraceIdRatio(rate)


def configure_telemetry(
    exporter: Optional[SpanExporter] = None,
) -> TracerProvider:
    sampler = _build_sampler() if exporter is None else None
    provider = TracerProvider(sampler=sampler)

    if exporter is not None:
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    else:
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


meter = metrics.get_meter(_SERVICE_NAME)

INGESTION_DURATION = meter.create_histogram(
    name="ingestion.duration_ms",
    description="Ingestion pipeline node duration in milliseconds",
    unit="ms",
)

LLM_EXTRACTION_DURATION = meter.create_histogram(
    name="llm.extraction_duration_ms",
    description="LLM extraction call duration in milliseconds",
    unit="ms",
)

NEO4J_TRANSACTION_DURATION = meter.create_histogram(
    name="neo4j.transaction_duration_ms",
    description="Neo4j transaction duration in milliseconds",
    unit="ms",
)

QUERY_DURATION = meter.create_histogram(
    name="query.duration_ms",
    description="Query pipeline node duration in milliseconds",
    unit="ms",
)
