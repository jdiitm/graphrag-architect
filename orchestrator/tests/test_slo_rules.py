from __future__ import annotations

import pytest

from orchestrator.app.slo_rules import (
    PRODUCTION_SLOS,
    ErrorBudget,
    SLODefinition,
    calculate_error_budget,
    determine_budget_status,
    generate_prometheus_rules,
)

EXPECTED_SLO_NAMES = {
    "ingestion_availability",
    "query_availability",
    "query_latency_p99_vector",
    "query_latency_p99_graph",
    "ingestion_freshness_p99",
    "dlq_error_rate",
}


class TestSLODefinitionsExist:
    def test_all_required_slos_present(self) -> None:
        actual_names = {slo.name for slo in PRODUCTION_SLOS}
        assert actual_names == EXPECTED_SLO_NAMES

    def test_slo_definitions_are_frozen(self) -> None:
        slo = PRODUCTION_SLOS[0]
        with pytest.raises(AttributeError):
            slo.name = "tampered"  # type: ignore[misc]


class TestSLOThresholds:
    def _slo_by_name(self, name: str) -> SLODefinition:
        matches = [s for s in PRODUCTION_SLOS if s.name == name]
        assert len(matches) == 1, f"Expected exactly one SLO named {name}"
        return matches[0]

    def test_ingestion_availability_target(self) -> None:
        slo = self._slo_by_name("ingestion_availability")
        assert slo.target == pytest.approx(0.999)

    def test_query_availability_target(self) -> None:
        slo = self._slo_by_name("query_availability")
        assert slo.target == pytest.approx(0.9995)

    def test_query_latency_vector_target(self) -> None:
        slo = self._slo_by_name("query_latency_p99_vector")
        assert slo.target == pytest.approx(0.995)

    def test_query_latency_graph_target(self) -> None:
        slo = self._slo_by_name("query_latency_p99_graph")
        assert slo.target == pytest.approx(0.99)

    def test_ingestion_freshness_target(self) -> None:
        slo = self._slo_by_name("ingestion_freshness_p99")
        assert slo.target == pytest.approx(0.99)

    def test_dlq_error_rate_target(self) -> None:
        slo = self._slo_by_name("dlq_error_rate")
        assert slo.target == pytest.approx(0.9999)

    def test_all_windows_are_30_days(self) -> None:
        thirty_days_seconds = 30 * 24 * 60 * 60
        for slo in PRODUCTION_SLOS:
            assert slo.window_seconds == thirty_days_seconds, (
                f"{slo.name} window is {slo.window_seconds}, expected {thirty_days_seconds}"
            )

    def test_all_slos_have_metric_query(self) -> None:
        for slo in PRODUCTION_SLOS:
            assert slo.metric_query, f"{slo.name} missing metric_query"


class TestErrorBudgetCalculation:
    def test_zero_consumed_budget(self) -> None:
        budget = calculate_error_budget("ingestion_availability", 0.999, 0.0)
        assert budget.slo_name == "ingestion_availability"
        assert budget.target == pytest.approx(0.999)
        assert budget.consumed_ratio == pytest.approx(0.0)
        assert budget.remaining_ratio == pytest.approx(1.0)

    def test_half_consumed_budget(self) -> None:
        budget = calculate_error_budget("ingestion_availability", 0.999, 0.5)
        assert budget.remaining_ratio == pytest.approx(0.5)

    def test_fully_consumed_budget(self) -> None:
        budget = calculate_error_budget("ingestion_availability", 0.999, 1.0)
        assert budget.remaining_ratio == pytest.approx(0.0)

    def test_over_consumed_clamps_to_zero(self) -> None:
        budget = calculate_error_budget("ingestion_availability", 0.999, 1.5)
        assert budget.remaining_ratio == pytest.approx(0.0)

    def test_error_budget_is_frozen(self) -> None:
        budget = calculate_error_budget("test", 0.999, 0.5)
        with pytest.raises(AttributeError):
            budget.slo_name = "tampered"  # type: ignore[misc]


class TestBudgetStatusDetermination:
    def test_green_above_50_percent(self) -> None:
        assert determine_budget_status(0.75) == "green"
        assert determine_budget_status(0.51) == "green"
        assert determine_budget_status(1.0) == "green"

    def test_yellow_25_to_50_percent(self) -> None:
        assert determine_budget_status(0.50) == "yellow"
        assert determine_budget_status(0.30) == "yellow"
        assert determine_budget_status(0.25) == "yellow"

    def test_orange_5_to_25_percent(self) -> None:
        assert determine_budget_status(0.24) == "orange"
        assert determine_budget_status(0.10) == "orange"
        assert determine_budget_status(0.05) == "orange"

    def test_red_below_5_percent(self) -> None:
        assert determine_budget_status(0.04) == "red"
        assert determine_budget_status(0.01) == "red"

    def test_black_exhausted(self) -> None:
        assert determine_budget_status(0.0) == "black"

    def test_negative_remaining_is_black(self) -> None:
        assert determine_budget_status(-0.1) == "black"


class TestPrometheusRuleGeneration:
    def test_generates_yaml_string(self) -> None:
        rules_yaml = generate_prometheus_rules()
        assert isinstance(rules_yaml, str)
        assert len(rules_yaml) > 0

    def test_contains_all_slo_recording_rules(self) -> None:
        rules_yaml = generate_prometheus_rules()
        for slo in PRODUCTION_SLOS:
            assert f"slo:{slo.name}" in rules_yaml, (
                f"Missing recording rule for {slo.name}"
            )

    def test_contains_error_budget_rules(self) -> None:
        rules_yaml = generate_prometheus_rules()
        for slo in PRODUCTION_SLOS:
            assert f"slo:{slo.name}:error_budget_remaining" in rules_yaml, (
                f"Missing error budget rule for {slo.name}"
            )

    def test_error_budget_expression_uses_correct_formula(self) -> None:
        rules_yaml = generate_prometheus_rules()
        for slo in PRODUCTION_SLOS:
            correct_expr = (
                f"(slo:{slo.name} - {slo.target}) / (1 - {slo.target})"
            )
            assert correct_expr in rules_yaml, (
                f"Error budget for {slo.name} must use "
                f"(actual - target) / (1 - target), got no match for: {correct_expr}"
            )

    def test_error_budget_expression_rejects_old_formula(self) -> None:
        rules_yaml = generate_prometheus_rules()
        for slo in PRODUCTION_SLOS:
            wrong_expr = f"1 - (slo:{slo.name} / {slo.target})"
            assert wrong_expr not in rules_yaml, (
                f"Error budget for {slo.name} still uses wrong formula: {wrong_expr}"
            )

    def test_rules_group_name(self) -> None:
        rules_yaml = generate_prometheus_rules()
        assert "graphrag_slo_recording_rules" in rules_yaml
