from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.config import ExtractionConfig


def _minimal_config() -> ExtractionConfig:
    return ExtractionConfig(google_api_key="test-key")


class TestLLMErrorHierarchy:
    def test_llm_error_is_exception(self) -> None:
        from orchestrator.app.llm_provider import LLMError

        assert issubclass(LLMError, Exception)

    def test_provider_unavailable_error(self) -> None:
        from orchestrator.app.llm_provider import ProviderUnavailableError

        err = ProviderUnavailableError("gemini down")
        assert isinstance(err, Exception)
        assert "gemini down" in str(err)

    def test_provider_timeout_error(self) -> None:
        from orchestrator.app.llm_provider import ProviderTimeoutError

        err = ProviderTimeoutError("timed out")
        assert isinstance(err, Exception)


class TestFallbackChain:
    @pytest.mark.asyncio
    async def test_primary_success_no_fallback(self) -> None:
        from orchestrator.app.llm_provider import FallbackChain, StubProvider

        primary = StubProvider(ainvoke_response="primary-answer")
        fallback = StubProvider(ainvoke_response="fallback-answer")
        chain = FallbackChain(providers=[primary, fallback])
        result = await chain.ainvoke("test prompt")
        assert result == "primary-answer"

    @pytest.mark.asyncio
    async def test_fallback_on_primary_failure(self) -> None:
        from orchestrator.app.llm_provider import FallbackChain, LLMError

        failing = AsyncMock(side_effect=LLMError("boom"))
        failing.ainvoke = AsyncMock(side_effect=LLMError("primary down"))
        fallback = AsyncMock()
        fallback.ainvoke = AsyncMock(return_value="fallback-answer")

        chain = FallbackChain(providers=[failing, fallback])
        result = await chain.ainvoke("test")
        assert result == "fallback-answer"

    @pytest.mark.asyncio
    async def test_all_providers_fail_raises(self) -> None:
        from orchestrator.app.llm_provider import FallbackChain, LLMError

        p1 = AsyncMock()
        p1.ainvoke = AsyncMock(side_effect=LLMError("p1 down"))
        p2 = AsyncMock()
        p2.ainvoke = AsyncMock(side_effect=LLMError("p2 down"))

        chain = FallbackChain(providers=[p1, p2])
        with pytest.raises(LLMError):
            await chain.ainvoke("test")


class TestPerProviderCircuitBreaker:
    @pytest.mark.asyncio
    async def test_circuit_opens_after_failures(self) -> None:
        from orchestrator.app.llm_provider import (
            LLMError,
            ProviderWithCircuitBreaker,
            StubProvider,
        )

        inner = AsyncMock()
        inner.ainvoke = AsyncMock(side_effect=LLMError("fail"))
        inner.ainvoke_structured = AsyncMock(side_effect=LLMError("fail"))

        provider = ProviderWithCircuitBreaker(
            inner, failure_threshold=2, reset_timeout=60.0,
        )

        with pytest.raises(LLMError):
            await provider.ainvoke("p1")
        with pytest.raises(LLMError):
            await provider.ainvoke("p2")

        from orchestrator.app.llm_provider import ProviderUnavailableError

        with pytest.raises(ProviderUnavailableError):
            await provider.ainvoke("p3")

    @pytest.mark.asyncio
    async def test_success_resets_failure_count(self) -> None:
        from orchestrator.app.llm_provider import (
            LLMError,
            ProviderWithCircuitBreaker,
        )

        call_count = 0

        class FlakeyProvider:
            async def ainvoke(self, prompt: str) -> str:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise LLMError("first call fails")
                return "ok"

            async def ainvoke_structured(self, prompt: str, messages: list):
                return None

        provider = ProviderWithCircuitBreaker(
            FlakeyProvider(), failure_threshold=3, reset_timeout=60.0,
        )

        with pytest.raises(LLMError):
            await provider.ainvoke("first")

        result = await provider.ainvoke("second")
        assert result == "ok"
        assert provider._failure_count == 0


class TestCreateProviderWithFallback:
    def test_create_provider_still_works(self) -> None:
        from orchestrator.app.llm_provider import StubProvider, create_provider

        provider = create_provider("stub", _minimal_config())
        assert isinstance(provider, StubProvider)
