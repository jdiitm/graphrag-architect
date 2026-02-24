from __future__ import annotations

from orchestrator.app.context_manager import (
    TokenBudget,
    estimate_tokens,
    format_context_for_prompt,
    truncate_context,
)


class TestTokenBudget:
    def test_defaults(self) -> None:
        budget = TokenBudget()
        assert budget.max_context_tokens == 32_000
        assert budget.max_results == 50

    def test_custom_values(self) -> None:
        budget = TokenBudget(max_context_tokens=1000, max_results=5)
        assert budget.max_context_tokens == 1000
        assert budget.max_results == 5


class TestEstimateTokens:
    def test_empty_string_returns_one(self) -> None:
        assert estimate_tokens("") == 1

    def test_short_string(self) -> None:
        assert estimate_tokens("hello") >= 1

    def test_long_string_proportional(self) -> None:
        text = "a" * 400
        tokens = estimate_tokens(text)
        assert tokens == 100


class TestTruncateContext:
    def test_empty_list_returns_empty(self) -> None:
        budget = TokenBudget()
        assert truncate_context([], budget) == []

    def test_within_budget_returns_all(self) -> None:
        budget = TokenBudget(max_context_tokens=100_000, max_results=50)
        candidates = [{"name": f"svc-{i}"} for i in range(5)]
        result = truncate_context(candidates, budget)
        assert len(result) == 5

    def test_max_results_caps_output(self) -> None:
        budget = TokenBudget(max_context_tokens=100_000, max_results=3)
        candidates = [{"name": f"svc-{i}"} for i in range(10)]
        result = truncate_context(candidates, budget)
        assert len(result) <= 3

    def test_token_limit_truncates(self) -> None:
        budget = TokenBudget(max_context_tokens=10, max_results=100)
        candidates = [{"data": "x" * 200} for _ in range(5)]
        result = truncate_context(candidates, budget)
        assert len(result) < 5

    def test_preserves_order(self) -> None:
        budget = TokenBudget(max_context_tokens=100_000, max_results=50)
        candidates = [{"id": i} for i in range(5)]
        result = truncate_context(candidates, budget)
        assert [c["id"] for c in result] == [0, 1, 2, 3, 4]


class TestFormatContextForPrompt:

    def test_returns_string(self) -> None:
        context = [{"name": "auth-service", "type": "Service"}]
        result = format_context_for_prompt(context)
        assert isinstance(result, str)

    def test_includes_field_labels(self) -> None:
        context = [{"name": "auth-service", "language": "go"}]
        result = format_context_for_prompt(context)
        assert "name" in result.lower()
        assert "auth-service" in result

    def test_no_raw_dict_repr(self) -> None:
        context = [{"name": "svc-a"}, {"name": "svc-b"}]
        result = format_context_for_prompt(context)
        assert "{'name'" not in result, (
            "Context must be formatted as structured text, not raw dict repr"
        )

    def test_empty_context_returns_empty(self) -> None:
        result = format_context_for_prompt([])
        assert result == ""

    def test_truncates_large_values(self) -> None:
        context = [{"data": "x" * 5000}]
        result = format_context_for_prompt(context, max_chars_per_value=200)
        assert len(result) < 5000

    def test_integrates_with_token_budget(self) -> None:
        budget = TokenBudget(max_context_tokens=100, max_results=2)
        context = [{"name": f"svc-{i}"} for i in range(10)]
        truncated = truncate_context(context, budget)
        result = format_context_for_prompt(truncated)
        assert isinstance(result, str)
        assert len(result) > 0
