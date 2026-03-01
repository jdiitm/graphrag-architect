from __future__ import annotations

import pytest

from orchestrator.app.context_manager import (
    ContextBudgetExceededError,
    TokenBudget,
    enforce_token_ceiling,
    truncate_context,
)


class TestContextBudgetExceededError:

    def test_exception_exists(self) -> None:
        assert issubclass(ContextBudgetExceededError, Exception)

    def test_raised_with_message(self) -> None:
        with pytest.raises(ContextBudgetExceededError, match="exceeds"):
            raise ContextBudgetExceededError("Context exceeds token budget")


class TestEnforceTokenCeiling:

    def test_function_exists(self) -> None:
        assert callable(enforce_token_ceiling)

    def test_passes_when_within_budget(self) -> None:
        candidates = [{"name": "svc-a", "score": 0.9}]
        budget = TokenBudget(max_context_tokens=32_000, max_results=50)
        enforce_token_ceiling(candidates, budget)

    def test_raises_when_single_giant_candidate(self) -> None:
        giant = {"description": "x" * 200_000}
        budget = TokenBudget(max_context_tokens=100, max_results=50)
        with pytest.raises(ContextBudgetExceededError):
            enforce_token_ceiling([giant], budget)

    def test_truncation_prevents_ceiling_breach(self) -> None:
        candidates = [{"data": "word " * 500} for _ in range(100)]
        budget = TokenBudget(max_context_tokens=1_000, max_results=50)
        truncated = truncate_context(candidates, budget)
        enforce_token_ceiling(truncated, budget)
