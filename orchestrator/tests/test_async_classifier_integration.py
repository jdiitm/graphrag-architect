import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.prompt_sanitizer import InjectionResult


class TestAsyncClassifierIntegration:

    def test_classify_async_function_exists(self) -> None:
        from orchestrator.app.query_engine import _classify_async
        assert asyncio.iscoroutinefunction(_classify_async)

    @pytest.mark.asyncio
    async def test_classify_async_delegates_to_thread(self) -> None:
        from orchestrator.app.query_engine import _classify_async

        expected_result = InjectionResult(
            score=0.1, detected_patterns=[], is_flagged=False,
        )
        with patch(
            "orchestrator.app.query_engine.asyncio.to_thread",
            new_callable=AsyncMock,
            return_value=expected_result,
        ) as mock_to_thread:
            result = await _classify_async("safe text")

        mock_to_thread.assert_called_once()
        assert result.score == 0.1
        assert not result.is_flagged

    @pytest.mark.asyncio
    async def test_synthesis_guardrails_use_async_classify(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PROMPT_GUARDRAILS_ENABLED", "true")
        safe_result = InjectionResult(
            score=0.0, detected_patterns=[], is_flagged=False,
        )
        with patch(
            "orchestrator.app.query_engine._classify_async",
            new_callable=AsyncMock,
            return_value=safe_result,
        ) as mock_classify, patch(
            "orchestrator.app.query_engine._build_synthesis_provider",
        ) as mock_provider_builder:
            mock_provider = AsyncMock()
            mock_provider.ainvoke_messages.return_value = "answer"
            mock_provider_builder.return_value = mock_provider

            from orchestrator.app.query_engine import _raw_llm_synthesize
            await _raw_llm_synthesize("test query", [{"key": "val"}])

        mock_classify.assert_called_once()

    @pytest.mark.asyncio
    async def test_streaming_guardrails_use_async_classify(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PROMPT_GUARDRAILS_ENABLED", "true")
        safe_result = InjectionResult(
            score=0.0, detected_patterns=[], is_flagged=False,
        )

        mock_llm = AsyncMock()

        async def fake_stream(*_args, **_kwargs):
            yield MagicMock(content="chunk")

        mock_llm.astream = fake_stream

        with patch(
            "orchestrator.app.query_engine._classify_async",
            new_callable=AsyncMock,
            return_value=safe_result,
        ) as mock_classify, patch(
            "langchain_google_genai.ChatGoogleGenerativeAI",
            return_value=mock_llm,
        ):
            from orchestrator.app.query_engine import _raw_llm_synthesize_stream
            chunks = []
            async for chunk in _raw_llm_synthesize_stream(
                "test query", [{"key": "val"}],
            ):
                chunks.append(chunk)

        mock_classify.assert_called_once()

    @pytest.mark.asyncio
    async def test_flagged_content_still_logs_warning(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PROMPT_GUARDRAILS_ENABLED", "true")
        flagged_result = InjectionResult(
            score=0.9,
            detected_patterns=["instruction_override"],
            is_flagged=True,
        )
        with patch(
            "orchestrator.app.query_engine._classify_async",
            new_callable=AsyncMock,
            return_value=flagged_result,
        ), patch(
            "orchestrator.app.query_engine._build_synthesis_provider",
        ) as mock_provider_builder, patch(
            "orchestrator.app.query_engine._query_logger",
        ) as mock_logger:
            mock_provider = AsyncMock()
            mock_provider.ainvoke_messages.return_value = "answer"
            mock_provider_builder.return_value = mock_provider

            from orchestrator.app.query_engine import _raw_llm_synthesize
            await _raw_llm_synthesize("test query", [{"key": "val"}])

        mock_logger.warning.assert_called()
        call_args = str(mock_logger.warning.call_args)
        assert "injection" in call_args.lower() or "flagged" in call_args.lower() \
            or "score" in call_args.lower()
