from __future__ import annotations

import logging
from typing import Any, Dict, List

import pytest

from orchestrator.app.context_manager import (
    ContextBudgetExceededError,
    TokenBudget,
    format_context_for_prompt,
    estimate_tokens,
)


def _make_record(label: str, size: int = 50) -> Dict[str, Any]:
    return {"name": label, "data": "x" * size}


class TestFormatContextTokenGuard:

    def test_context_within_budget_passes_through(self) -> None:
        records = [_make_record(f"svc-{i}") for i in range(3)]
        budget = TokenBudget(max_context_tokens=32_000, max_results=50)
        result = format_context_for_prompt(records, budget=budget)
        assert result.content
        assert result.delimiter

    def test_context_exceeding_budget_truncated(self) -> None:
        records = [_make_record(f"svc-{i}", size=500) for i in range(200)]
        budget = TokenBudget(max_context_tokens=2_000, max_results=200)
        result = format_context_for_prompt(records, budget=budget)
        token_count = estimate_tokens(result.content)
        assert token_count <= budget.max_context_tokens

    def test_truncation_preserves_rank_order(self) -> None:
        records = [
            {"name": f"svc-{i}", "score": float(100 - i), "data": "x" * 300}
            for i in range(50)
        ]
        budget = TokenBudget(max_context_tokens=1_000, max_results=50)
        result = format_context_for_prompt(records, budget=budget)
        assert "svc-0" in result.content
        assert "svc-49" not in result.content

    def test_single_record_exceeding_budget_raises(self) -> None:
        records = [{"name": "huge", "data": "x" * 50_000}]
        budget = TokenBudget(max_context_tokens=100, max_results=50)
        with pytest.raises(ContextBudgetExceededError):
            format_context_for_prompt(records, budget=budget)

    def test_truncation_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        records = [_make_record(f"svc-{i}", size=500) for i in range(100)]
        budget = TokenBudget(max_context_tokens=2_000, max_results=100)
        with caplog.at_level(logging.WARNING, logger="orchestrator.app.context_manager"):
            format_context_for_prompt(records, budget=budget)
        assert any("token budget" in msg.lower() for msg in caplog.messages)
