from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

_THIRTY_DAYS_SECONDS = 30 * 24 * 60 * 60


@dataclass(frozen=True)
class SLODefinition:
    name: str
    target: float
    window_seconds: int
    metric_query: str


@dataclass(frozen=True)
class ErrorBudget:
    slo_name: str
    target: float
    consumed_ratio: float
    remaining_ratio: float
    status: str


PRODUCTION_SLOS: tuple[SLODefinition, ...] = (
    SLODefinition(
        name="ingestion_availability",
        target=0.999,
        window_seconds=_THIRTY_DAYS_SECONDS,
        metric_query=(
            "sum(rate(http_requests_total{endpoint='/ingest',status=~'2..'}[5m])) "
            "/ sum(rate(http_requests_total{endpoint='/ingest',status!~'4..'}[5m]))"
        ),
    ),
    SLODefinition(
        name="query_availability",
        target=0.9995,
        window_seconds=_THIRTY_DAYS_SECONDS,
        metric_query=(
            "sum(rate(http_requests_total{endpoint='/query',status=~'2..'}[5m])) "
            "/ sum(rate(http_requests_total{endpoint='/query',status!~'4..'}[5m]))"
        ),
    ),
    SLODefinition(
        name="query_latency_p99_vector",
        target=0.995,
        window_seconds=_THIRTY_DAYS_SECONDS,
        metric_query=(
            "sum(rate(http_request_duration_seconds_bucket{endpoint='/query',"
            "strategy='vector',le='0.5'}[5m])) "
            "/ sum(rate(http_request_duration_seconds_count{endpoint='/query',"
            "strategy='vector'}[5m]))"
        ),
    ),
    SLODefinition(
        name="query_latency_p99_graph",
        target=0.99,
        window_seconds=_THIRTY_DAYS_SECONDS,
        metric_query=(
            "sum(rate(http_request_duration_seconds_bucket{endpoint='/query',"
            "strategy='graph',le='3.0'}[5m])) "
            "/ sum(rate(http_request_duration_seconds_count{endpoint='/query',"
            "strategy='graph'}[5m]))"
        ),
    ),
    SLODefinition(
        name="ingestion_freshness_p99",
        target=0.99,
        window_seconds=_THIRTY_DAYS_SECONDS,
        metric_query=(
            "sum(rate(ingestion_duration_ms_bucket{le='10000'}[5m])) "
            "/ sum(rate(ingestion_duration_ms_count[5m]))"
        ),
    ),
    SLODefinition(
        name="dlq_error_rate",
        target=0.9999,
        window_seconds=_THIRTY_DAYS_SECONDS,
        metric_query=(
            "1 - (sum(rate(dlq_messages_total[5m])) "
            "/ sum(rate(documents_ingested_total[5m])))"
        ),
    ),
)


def calculate_error_budget(
    slo_name: str,
    target: float,
    consumed_ratio: float,
) -> ErrorBudget:
    remaining = max(0.0, 1.0 - consumed_ratio)
    status = determine_budget_status(remaining)
    return ErrorBudget(
        slo_name=slo_name,
        target=target,
        consumed_ratio=consumed_ratio,
        remaining_ratio=remaining,
        status=status,
    )


def determine_budget_status(remaining_ratio: float) -> str:
    if remaining_ratio <= 0.0:
        return "black"
    if remaining_ratio < 0.05:
        return "red"
    if remaining_ratio < 0.25:
        return "orange"
    if remaining_ratio <= 0.50:
        return "yellow"
    return "green"


def generate_prometheus_rules() -> str:
    return _render_rules(PRODUCTION_SLOS)


def _render_rules(slos: Sequence[SLODefinition]) -> str:
    lines: list[str] = []
    lines.append("groups:")
    lines.append("  - name: graphrag_slo_recording_rules")
    lines.append("    interval: 60s")
    lines.append("    rules:")

    for slo in slos:
        lines.append(f"      - record: slo:{slo.name}")
        lines.append(f"        expr: {slo.metric_query}")

        error_budget_expr = f"1 - (slo:{slo.name} / {slo.target})"
        lines.append(f"      - record: slo:{slo.name}:error_budget_remaining")
        lines.append(f"        expr: clamp_min({error_budget_expr}, 0)")

    return "\n".join(lines) + "\n"
