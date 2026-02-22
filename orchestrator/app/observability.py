from __future__ import annotations

import os
from typing import Optional

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
    SpanExporter,
)
from opentelemetry.sdk.trace.sampling import ParentBasedTraceIdRatio

_SERVICE_NAME = "graphrag-orchestrator"
_STATE: dict[str, trace.Tracer] = {}


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
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint))
        )

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
