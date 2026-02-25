from __future__ import annotations

import pytest

from orchestrator.app.observability import ErrorBudgetTracker


class TestErrorBudgetTracker:
    def test_initial_state_has_full_budget(self):
        tracker = ErrorBudgetTracker(slo_target=0.995)
        assert tracker.budget_remaining == pytest.approx(0.005)
        assert tracker.budget_exhausted is False

    def test_error_rate_calculated_correctly(self):
        tracker = ErrorBudgetTracker(slo_target=0.99)
        for _ in range(95):
            tracker.record_request(is_error=False)
        for _ in range(5):
            tracker.record_request(is_error=True)
        assert tracker.error_rate == pytest.approx(0.05)

    def test_budget_exhausted_when_errors_exceed_target(self):
        tracker = ErrorBudgetTracker(slo_target=0.99)
        for _ in range(98):
            tracker.record_request(is_error=False)
        for _ in range(2):
            tracker.record_request(is_error=True)
        assert tracker.error_rate == pytest.approx(0.02)
        assert tracker.budget_exhausted is True

    def test_budget_remaining_positive_within_slo(self):
        tracker = ErrorBudgetTracker(slo_target=0.995)
        for _ in range(999):
            tracker.record_request(is_error=False)
        tracker.record_request(is_error=True)
        assert tracker.error_rate == pytest.approx(0.001)
        assert tracker.budget_remaining > 0

    def test_reset_clears_counters(self):
        tracker = ErrorBudgetTracker(slo_target=0.99)
        tracker.record_request(is_error=True)
        tracker.reset()
        assert tracker.total_requests == 0
        assert tracker.error_count == 0
        assert tracker.error_rate == 0.0

    def test_zero_requests_has_zero_error_rate(self):
        tracker = ErrorBudgetTracker(slo_target=0.99)
        assert tracker.error_rate == 0.0


class TestSLORecordingRulesExist:
    def test_alerting_yaml_has_recording_rules(self):
        import yaml
        with open("infrastructure/k8s/alerting.yaml") as fh:
            docs = list(yaml.safe_load_all(fh))
        rule = docs[0]
        group_names = [g["name"] for g in rule["spec"]["groups"]]
        assert "graphrag.slo.recording" in group_names
        assert "graphrag.slo.alerts" in group_names

    def test_recording_rules_define_success_rate(self):
        import yaml
        with open("infrastructure/k8s/alerting.yaml") as fh:
            docs = list(yaml.safe_load_all(fh))
        rule = docs[0]
        recording_group = next(
            g for g in rule["spec"]["groups"]
            if g["name"] == "graphrag.slo.recording"
        )
        record_names = [r["record"] for r in recording_group["rules"]]
        assert "graphrag:query_success_rate:5m" in record_names
        assert "graphrag:query_latency_slo:5m" in record_names
        assert "graphrag:ingestion_success_rate:5m" in record_names
