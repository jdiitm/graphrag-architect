from pathlib import Path

import yaml
import pytest


ALERTING_YAML_PATH = (
    Path(__file__).resolve().parents[2]
    / "infrastructure"
    / "k8s"
    / "alerting.yaml"
)


@pytest.fixture(name="alert_rules")
def _alert_rules() -> list[dict]:
    content = ALERTING_YAML_PATH.read_text(encoding="utf-8")
    manifest = yaml.safe_load(content)
    rules: list[dict] = []
    for group in manifest["spec"]["groups"]:
        rules.extend(group["rules"])
    return rules


def _find_alert(rules: list[dict], alert_name: str) -> dict | None:
    for rule in rules:
        if rule.get("alert") == alert_name:
            return rule
    return None


class TestDLQSinkFailureAlert:
    def test_alert_exists(self, alert_rules: list[dict]) -> None:
        alert = _find_alert(alert_rules, "DLQSinkFailure")
        assert alert is not None, (
            "Expected DLQSinkFailure alert in alerting.yaml"
        )

    def test_targets_correct_metric(self, alert_rules: list[dict]) -> None:
        alert = _find_alert(alert_rules, "DLQSinkFailure")
        assert alert is not None
        assert "ingestion_dlq_sink_error_total" in alert["expr"]

    def test_severity_is_critical(self, alert_rules: list[dict]) -> None:
        alert = _find_alert(alert_rules, "DLQSinkFailure")
        assert alert is not None
        assert alert["labels"]["severity"] == "critical"

    def test_routes_to_pagerduty(self, alert_rules: list[dict]) -> None:
        alert = _find_alert(alert_rules, "DLQSinkFailure")
        assert alert is not None
        assert alert["labels"]["channel"] == "pagerduty"

    def test_has_for_duration(self, alert_rules: list[dict]) -> None:
        alert = _find_alert(alert_rules, "DLQSinkFailure")
        assert alert is not None
        assert "for" in alert, "Alert must have a 'for' clause"

    def test_has_summary_annotation(self, alert_rules: list[dict]) -> None:
        alert = _find_alert(alert_rules, "DLQSinkFailure")
        assert alert is not None
        assert "summary" in alert.get("annotations", {})

    def test_has_description_annotation(self, alert_rules: list[dict]) -> None:
        alert = _find_alert(alert_rules, "DLQSinkFailure")
        assert alert is not None
        assert "description" in alert.get("annotations", {})


class TestAllDLQMetricsCovered:
    def test_dlq_routed_has_alert(self, alert_rules: list[dict]) -> None:
        expressions = [r["expr"] for r in alert_rules]
        covered = any("ingestion_dlq_routed_total" in e for e in expressions)
        assert covered, (
            "Expected an alert covering ingestion_dlq_routed_total"
        )

    def test_dlq_sink_error_has_alert(self, alert_rules: list[dict]) -> None:
        expressions = [r["expr"] for r in alert_rules]
        covered = any(
            "ingestion_dlq_sink_error_total" in e for e in expressions
        )
        assert covered, (
            "Expected an alert covering ingestion_dlq_sink_error_total"
        )
