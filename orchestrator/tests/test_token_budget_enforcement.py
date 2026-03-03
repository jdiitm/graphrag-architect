from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.context_manager import TokenBudget, format_context_for_prompt


class TestFormatContextWithBudget:
    def test_budget_limits_output(self) -> None:
        large_context = [
            {"name": f"service_{i}", "description": "x" * 500}
            for i in range(200)
        ]
        budget = TokenBudget(max_context_tokens=500, max_results=10)
        result = format_context_for_prompt(large_context, budget=budget)
        assert result.content != ""

    def test_no_budget_returns_everything(self) -> None:
        context = [
            {"name": f"service_{i}", "description": "data"}
            for i in range(5)
        ]
        result = format_context_for_prompt(context, budget=None)
        assert "service_0" in result.content
        assert "service_4" in result.content


class TestSynthesisCallSitesUseBudget:
    @pytest.mark.asyncio
    async def test_raw_llm_synthesize_passes_budget(self) -> None:
        with patch(
            "orchestrator.app.query_engine.format_context_for_prompt",
            wraps=format_context_for_prompt,
        ) as mock_fmt, patch(
            "orchestrator.app.query_engine._build_synthesis_provider",
        ) as mock_provider, patch(
            "orchestrator.app.query_engine._prompt_guardrails_enabled",
            return_value=False,
        ):
            mock_llm = AsyncMock()
            mock_llm.ainvoke.return_value = MagicMock(content="answer")
            mock_provider.return_value = mock_llm

            from orchestrator.app.query_engine import _raw_llm_synthesize

            await _raw_llm_synthesize("test query", [{"data": "value"}])

            mock_fmt.assert_called_once()
            call_kwargs = mock_fmt.call_args
            assert call_kwargs.kwargs.get("budget") is not None or (
                len(call_kwargs.args) > 1 and call_kwargs.args[1] is not None
            ), "format_context_for_prompt must be called with a budget"

    @pytest.mark.asyncio
    async def test_raw_llm_synthesize_stream_passes_budget(self) -> None:
        with patch(
            "orchestrator.app.query_engine.format_context_for_prompt",
            wraps=format_context_for_prompt,
        ) as mock_fmt, patch(
            "orchestrator.app.query_engine._build_synthesis_provider",
        ) as mock_provider, patch(
            "orchestrator.app.query_engine._prompt_guardrails_enabled",
            return_value=False,
        ):
            mock_llm = AsyncMock()

            async def _fake_stream(*_args, **_kwargs):
                yield MagicMock(content="chunk")

            mock_llm.astream = _fake_stream
            mock_provider.return_value = mock_llm

            from orchestrator.app.query_engine import _raw_llm_synthesize_stream

            chunks = []
            async for chunk in _raw_llm_synthesize_stream(
                "test query", [{"data": "value"}],
            ):
                chunks.append(chunk)

            mock_fmt.assert_called_once()
            call_kwargs = mock_fmt.call_args
            assert call_kwargs.kwargs.get("budget") is not None or (
                len(call_kwargs.args) > 1 and call_kwargs.args[1] is not None
            ), "format_context_for_prompt must be called with a budget"
