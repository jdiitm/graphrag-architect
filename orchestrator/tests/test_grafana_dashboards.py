from __future__ import annotations

import json
from pathlib import Path

import pytest

GRAFANA_DIR = Path(__file__).resolve().parents[2] / "infrastructure" / "grafana"

REQUIRED_DASHBOARD_STEMS = {
    "ingestion-pipeline",
    "query-engine",
    "neo4j-health",
    "kafka-lag",
    "circuit-breaker",
    "tenant-usage",
    "slo-burn-rate",
    "dlq-monitoring",
}


class TestDashboardFilesExist:
    def test_grafana_directory_exists(self) -> None:
        assert GRAFANA_DIR.is_dir(), f"Missing directory: {GRAFANA_DIR}"

    def test_minimum_dashboard_count(self) -> None:
        json_files = list(GRAFANA_DIR.glob("*.json"))
        assert len(json_files) >= 8, (
            f"Expected at least 8 dashboards, found {len(json_files)}"
        )

    def test_all_required_dashboards_present(self) -> None:
        actual_stems = {f.stem for f in GRAFANA_DIR.glob("*.json")}
        missing = REQUIRED_DASHBOARD_STEMS - actual_stems
        assert not missing, f"Missing dashboards: {missing}"


class TestDashboardJsonValidity:
    @pytest.fixture()
    def dashboard_files(self) -> list[Path]:
        return sorted(GRAFANA_DIR.glob("*.json"))

    def test_all_files_are_valid_json(self, dashboard_files: list[Path]) -> None:
        for path in dashboard_files:
            raw = path.read_text(encoding="utf-8")
            try:
                json.loads(raw)
            except json.JSONDecodeError as exc:
                pytest.fail(f"{path.name} is not valid JSON: {exc}")


class TestDashboardRequiredFields:
    @pytest.fixture()
    def dashboards(self) -> list[tuple[str, dict]]:
        result: list[tuple[str, dict]] = []
        for path in sorted(GRAFANA_DIR.glob("*.json")):
            raw = path.read_text(encoding="utf-8")
            result.append((path.name, json.loads(raw)))
        return result

    def test_each_dashboard_has_title(self, dashboards: list[tuple[str, dict]]) -> None:
        for name, data in dashboards:
            assert "title" in data, f"{name} missing 'title'"
            assert isinstance(data["title"], str)
            assert len(data["title"]) > 0

    def test_each_dashboard_has_uid(self, dashboards: list[tuple[str, dict]]) -> None:
        for name, data in dashboards:
            assert "uid" in data, f"{name} missing 'uid'"

    def test_each_dashboard_has_panels(self, dashboards: list[tuple[str, dict]]) -> None:
        for name, data in dashboards:
            assert "panels" in data, f"{name} missing 'panels'"
            assert isinstance(data["panels"], list)
            assert len(data["panels"]) > 0, f"{name} has no panels"

    def test_each_dashboard_has_tags(self, dashboards: list[tuple[str, dict]]) -> None:
        for name, data in dashboards:
            assert "tags" in data, f"{name} missing 'tags'"
            assert isinstance(data["tags"], list)

    def test_uids_are_unique(self, dashboards: list[tuple[str, dict]]) -> None:
        uids = [data["uid"] for _, data in dashboards]
        assert len(uids) == len(set(uids)), "Dashboard UIDs are not unique"


class TestDashboardPanelQueries:
    KNOWN_METRICS = {
        "ingestion.duration_ms",
        "llm.extraction_duration_ms",
        "neo4j.transaction_duration_ms",
        "query.duration_ms",
        "query.total",
        "query.errors",
        "ingestion.total",
        "ingestion.errors",
        "embedding.fallback_total",
        "http_requests_total",
        "http_request_duration_seconds",
        "neo4j_connection_pool_active",
        "neo4j_transaction_duration_seconds",
        "kafka_consumer_lag_messages",
        "llm_request_duration_seconds",
        "llm_request_tokens_total",
        "llm_request_errors_total",
        "circuit_breaker_state",
        "cache_hits_total",
        "cache_misses_total",
        "documents_ingested_total",
        "entities_extracted_total",
        "graph_nodes_total",
        "graph_relationships_total",
        "queries_executed_total",
        "dlq_messages_total",
    }

    def _extract_panel_exprs(self, dashboard: dict) -> list[str]:
        exprs: list[str] = []
        for panel in dashboard.get("panels", []):
            for target in panel.get("targets", []):
                expr = target.get("expr", "")
                if expr:
                    exprs.append(expr)
        return exprs

    def test_each_dashboard_has_prometheus_queries(self) -> None:
        for path in sorted(GRAFANA_DIR.glob("*.json")):
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
            exprs = self._extract_panel_exprs(data)
            assert len(exprs) > 0, (
                f"{path.name} has no Prometheus query expressions in panels"
            )

    def test_panel_queries_reference_known_metrics(self) -> None:
        all_exprs: list[tuple[str, str]] = []
        for path in sorted(GRAFANA_DIR.glob("*.json")):
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
            for expr in self._extract_panel_exprs(data):
                all_exprs.append((path.name, expr))

        for filename, expr in all_exprs:
            references_known = any(
                metric.replace(".", "_") in expr or metric in expr
                for metric in self.KNOWN_METRICS
            )
            assert references_known, (
                f"{filename} expr '{expr}' does not reference any known metric"
            )
