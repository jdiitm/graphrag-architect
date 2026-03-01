from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.circuit_breaker import CircuitOpenError
from orchestrator.app.config import ExtractionConfig
from orchestrator.app.llm_provider import (
    FallbackChain,
    LLMError,
    LLMProvider,
    ProviderWithCircuitBreaker,
)


def _extraction_config(**overrides) -> ExtractionConfig:
    defaults = {
        "google_api_key": "test-key",
        "anthropic_api_key": "test-key",
        "llm_provider": "gemini",
    }
    defaults.update(overrides)
    return ExtractionConfig(**defaults)


class TestBuildSynthesisProvider:

    def test_returns_llm_provider_not_raw_langchain(self) -> None:
        from orchestrator.app.query_engine import _build_synthesis_provider

        with (
            patch(
                "orchestrator.app.llm_provider.GeminiProvider.__init__",
                return_value=None,
            ),
            patch(
                "orchestrator.app.llm_provider.ClaudeProvider.__init__",
                return_value=None,
            ),
        ):
            provider = _build_synthesis_provider()

        assert isinstance(provider, (FallbackChain, ProviderWithCircuitBreaker)), (
            "_build_synthesis_provider must return an LLMProvider "
            "(FallbackChain or ProviderWithCircuitBreaker), "
            f"got {type(provider).__name__}"
        )

    def test_does_not_return_chat_google_generative_ai(self) -> None:
        from langchain_google_genai import ChatGoogleGenerativeAI

        from orchestrator.app.query_engine import _build_synthesis_provider

        with (
            patch(
                "orchestrator.app.llm_provider.GeminiProvider.__init__",
                return_value=None,
            ),
            patch(
                "orchestrator.app.llm_provider.ClaudeProvider.__init__",
                return_value=None,
            ),
        ):
            provider = _build_synthesis_provider()

        assert not isinstance(provider, ChatGoogleGenerativeAI), (
            "_build_synthesis_provider must NOT return raw ChatGoogleGenerativeAI. "
            "It must use the FallbackChain from llm_provider.py."
        )


class TestSynthesisUsesFallbackChain:

    @pytest.mark.asyncio
    async def test_raw_llm_synthesize_calls_provider_ainvoke_messages(self) -> None:
        from orchestrator.app.query_engine import _raw_llm_synthesize

        mock_provider = AsyncMock()
        mock_provider.ainvoke_messages = AsyncMock(return_value="test answer")

        with (
            patch(
                "orchestrator.app.query_engine._build_synthesis_provider",
                return_value=mock_provider,
            ),
            patch(
                "orchestrator.app.query_engine._prompt_guardrails_enabled",
                return_value=False,
            ),
        ):
            result = await _raw_llm_synthesize(
                "What port does auth listen on?",
                [{"name": "auth-service", "port": 8080}],
            )

        assert result == "test answer"
        mock_provider.ainvoke_messages.assert_called_once()
        call_args = mock_provider.ainvoke_messages.call_args[0]
        messages = call_args[0]
        assert len(messages) == 2

    @pytest.mark.asyncio
    async def test_primary_fails_fallback_succeeds(self) -> None:
        from orchestrator.app.query_engine import _llm_synthesize

        primary = AsyncMock()
        primary.ainvoke_messages = AsyncMock(
            side_effect=LLMError("gemini rate limited"),
        )
        fallback = AsyncMock()
        fallback.ainvoke_messages = AsyncMock(
            return_value="fallback answer",
        )
        chain = FallbackChain(providers=[primary, fallback])

        with (
            patch(
                "orchestrator.app.query_engine._build_synthesis_provider",
                return_value=chain,
            ),
            patch(
                "orchestrator.app.query_engine._prompt_guardrails_enabled",
                return_value=False,
            ),
            patch(
                "orchestrator.app.query_engine._CB_LLM_GLOBAL",
            ) as mock_cb,
        ):
            async def passthrough_call(
                tenant_id, fn, *args, **kwargs
            ):
                return await fn(*args, **kwargs)

            mock_cb.call = AsyncMock(side_effect=passthrough_call)
            result = await _llm_synthesize(
                "test query",
                [{"name": "svc"}],
                tenant_id="t1",
            )

        assert result == "fallback answer"

    @pytest.mark.asyncio
    async def test_all_providers_fail_returns_degraded_response(self) -> None:
        from orchestrator.app.query_engine import _llm_synthesize

        mock_cb = AsyncMock()
        mock_cb.call = AsyncMock(
            side_effect=LLMError("All 2 providers failed"),
        )

        with patch(
            "orchestrator.app.query_engine._CB_LLM_GLOBAL", mock_cb,
        ):
            result = await _llm_synthesize(
                "test query",
                [{"name": "svc"}],
                tenant_id="t1",
            )

        assert "unavailable" in result.lower(), (
            "When all LLM providers fail (LLMError), synthesis must return "
            f"a degraded response, got: {result}"
        )

    @pytest.mark.asyncio
    async def test_circuit_open_still_returns_degraded(self) -> None:
        from orchestrator.app.query_engine import _llm_synthesize

        mock_cb = AsyncMock()
        mock_cb.call = AsyncMock(side_effect=CircuitOpenError("open"))

        with patch(
            "orchestrator.app.query_engine._CB_LLM_GLOBAL", mock_cb,
        ):
            result = await _llm_synthesize(
                "test query", [{"name": "svc"}], tenant_id="t1",
            )

        assert "unavailable" in result.lower()


class TestAinvokeMessagesProtocol:

    @pytest.mark.asyncio
    async def test_fallback_chain_has_ainvoke_messages(self) -> None:
        p1 = AsyncMock()
        p1.ainvoke_messages = AsyncMock(return_value="answer")
        chain = FallbackChain(providers=[p1])
        result = await chain.ainvoke_messages([{"role": "user", "content": "hi"}])
        assert result == "answer"

    @pytest.mark.asyncio
    async def test_provider_with_circuit_breaker_has_ainvoke_messages(self) -> None:
        inner = AsyncMock()
        inner.ainvoke_messages = AsyncMock(return_value="answer")
        provider = ProviderWithCircuitBreaker(inner)
        result = await provider.ainvoke_messages([{"role": "user", "content": "hi"}])
        assert result == "answer"

    @pytest.mark.asyncio
    async def test_fallback_chain_ainvoke_messages_falls_back(self) -> None:
        p1 = AsyncMock()
        p1.ainvoke_messages = AsyncMock(side_effect=LLMError("p1 down"))
        p2 = AsyncMock()
        p2.ainvoke_messages = AsyncMock(return_value="p2 answer")

        chain = FallbackChain(providers=[p1, p2])
        result = await chain.ainvoke_messages([{"role": "user", "content": "hi"}])
        assert result == "p2 answer"
        p1.ainvoke_messages.assert_called_once()
        p2.ainvoke_messages.assert_called_once()


class TestExtractionUsesFallbackChain:

    def test_service_extractor_uses_failover_provider(self) -> None:
        from orchestrator.app.llm_extraction import ServiceExtractor

        config = _extraction_config()

        with (
            patch(
                "orchestrator.app.llm_extraction.create_provider_with_failover",
            ) as mock_create,
        ):
            mock_provider = MagicMock()
            mock_create.return_value = mock_provider
            extractor = ServiceExtractor(config)

        mock_create.assert_called_once_with(config)

    def test_service_extractor_does_not_use_raw_langchain(self) -> None:
        from orchestrator.app.llm_extraction import ServiceExtractor

        config = _extraction_config()

        with (
            patch(
                "orchestrator.app.llm_extraction.create_provider_with_failover",
            ) as mock_create,
        ):
            mock_provider = MagicMock()
            mock_create.return_value = mock_provider
            extractor = ServiceExtractor(config)

        assert not hasattr(extractor, "_raw_llm"), (
            "ServiceExtractor should not hold a raw ChatGoogleGenerativeAI "
            "instance. It should use create_provider_with_failover."
        )


class TestLLMJudgeUsesFallbackChain:

    def test_build_llm_judge_uses_provider(self) -> None:
        from orchestrator.app.query_engine import _build_llm_judge_fn

        mock_provider = AsyncMock()
        mock_provider.ainvoke = AsyncMock(return_value="high")

        with patch(
            "orchestrator.app.query_engine._build_synthesis_provider",
            return_value=mock_provider,
        ):
            judge_fn = _build_llm_judge_fn()

        assert judge_fn is not None


class TestQuerySynthesizerReExports:

    def test_exports_build_synthesis_provider(self) -> None:
        from orchestrator.app.query_synthesizer import _build_synthesis_provider

        assert callable(_build_synthesis_provider)
