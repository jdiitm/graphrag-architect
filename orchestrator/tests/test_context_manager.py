from __future__ import annotations

from orchestrator.app.context_manager import (
    ContextBlock,
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
        assert 10 <= tokens <= 200


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


class TestSemanticTruncation:

    def test_highest_scored_items_survive(self) -> None:
        budget = TokenBudget(max_context_tokens=100, max_results=2)
        candidates = [
            {"name": "low-svc", "score": 0.1},
            {"name": "high-svc", "score": 0.95},
            {"name": "mid-svc", "score": 0.5},
        ]
        result = truncate_context(candidates, budget)
        names = [c["name"] for c in result]
        assert "high-svc" in names

    def test_low_scored_items_dropped_first(self) -> None:
        budget = TokenBudget(max_context_tokens=60, max_results=50)
        candidates = [
            {"name": f"svc-{i}", "data": "x" * 40, "score": 0.1 * i}
            for i in range(10)
        ]
        result = truncate_context(candidates, budget)
        if len(result) < len(candidates):
            surviving_scores = [c["score"] for c in result]
            dropped_scores = [
                c["score"] for c in candidates if c not in result
            ]
            assert min(surviving_scores) >= max(dropped_scores)

    def test_graceful_when_no_scores(self) -> None:
        budget = TokenBudget(max_context_tokens=100_000, max_results=50)
        candidates = [{"name": f"svc-{i}"} for i in range(5)]
        result = truncate_context(candidates, budget)
        assert len(result) == 5


class TestFormatContextForPrompt:

    def test_returns_context_block(self) -> None:
        context = [{"name": "auth-service", "type": "Service"}]
        block = format_context_for_prompt(context)
        assert isinstance(block, ContextBlock)
        assert isinstance(block.content, str)
        assert isinstance(block.delimiter, str)
        assert len(block.delimiter) > 0

    def test_includes_field_labels(self) -> None:
        context = [{"name": "auth-service", "language": "go"}]
        block = format_context_for_prompt(context)
        assert "name" in block.content.lower()
        assert "auth-service" in block.content

    def test_no_raw_dict_repr(self) -> None:
        context = [{"name": "svc-a"}, {"name": "svc-b"}]
        block = format_context_for_prompt(context)
        assert "{'name'" not in block.content, (
            "Context must be formatted as structured text, not raw dict repr"
        )

    def test_empty_context_returns_empty(self) -> None:
        block = format_context_for_prompt([])
        assert block.content == ""
        assert block.delimiter == ""

    def test_truncates_large_values(self) -> None:
        context = [{"data": "x" * 5000}]
        block = format_context_for_prompt(context, max_chars_per_value=200)
        assert len(block.content) < 5000

    def test_integrates_with_token_budget(self) -> None:
        budget = TokenBudget(max_context_tokens=100, max_results=2)
        context = [{"name": f"svc-{i}"} for i in range(10)]
        truncated = truncate_context(context, budget)
        block = format_context_for_prompt(truncated)
        assert isinstance(block.content, str)
        assert len(block.content) > 0

    def test_delimiter_is_random_per_call(self) -> None:
        context = [{"name": "svc"}]
        block_a = format_context_for_prompt(context)
        block_b = format_context_for_prompt(context)
        assert block_a.delimiter != block_b.delimiter

    def test_content_wrapped_in_delimiter_tags(self) -> None:
        context = [{"name": "auth-service"}]
        block = format_context_for_prompt(context)
        assert block.content.startswith(f"<{block.delimiter}>")
        assert block.content.endswith(f"</{block.delimiter}>")

    def test_static_graph_context_tag_not_used(self) -> None:
        context = [{"name": "svc"}]
        block = format_context_for_prompt(context)
        assert "<graph_context>" not in block.content
        assert "</graph_context>" not in block.content
