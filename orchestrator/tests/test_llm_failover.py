from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.config import ExtractionConfig
from orchestrator.app.llm_provider import (
    ClaudeProvider,
    FallbackChain,
    LLMError,
    ProviderWithCircuitBreaker,
    StubProvider,
    create_provider,
    create_provider_with_failover,
)


def _config(**kwargs) -> ExtractionConfig:
    defaults = {"google_api_key": "test-key", "anthropic_api_key": "test-key"}
    defaults.update(kwargs)
    return ExtractionConfig(**defaults)


class TestCreateProviderClaudeSupport:

    def test_create_provider_accepts_claude(self) -> None:
        with patch("orchestrator.app.llm_provider.ClaudeProvider.__init__", return_value=None):
            provider = create_provider("claude", _config())
        assert isinstance(provider, ProviderWithCircuitBreaker)

    def test_create_provider_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown LLM provider"):
            create_provider("gpt-4", _config())


class TestCreateProviderWithFailover:

    def test_returns_fallback_chain_when_both_available(self) -> None:
        with (
            patch("orchestrator.app.llm_provider.GeminiProvider.__init__", return_value=None),
            patch("orchestrator.app.llm_provider.ClaudeProvider.__init__", return_value=None),
        ):
            provider = create_provider_with_failover(
                _config(llm_provider="gemini"),
            )
        assert isinstance(provider, FallbackChain)

    def test_returns_single_provider_when_fallback_unavailable(self) -> None:
        with (
            patch("orchestrator.app.llm_provider.GeminiProvider.__init__", return_value=None),
            patch(
                "orchestrator.app.llm_provider.ClaudeProvider.__init__",
                side_effect=ImportError("no anthropic"),
            ),
        ):
            provider = create_provider_with_failover(
                _config(llm_provider="gemini"),
            )
        assert isinstance(provider, ProviderWithCircuitBreaker)

    def test_raises_when_no_providers_available(self) -> None:
        with (
            patch(
                "orchestrator.app.llm_provider.GeminiProvider.__init__",
                side_effect=ImportError("no gemini"),
            ),
            patch(
                "orchestrator.app.llm_provider.ClaudeProvider.__init__",
                side_effect=ImportError("no claude"),
            ),
        ):
            with pytest.raises(LLMError, match="No LLM providers"):
                create_provider_with_failover(
                    _config(llm_provider="gemini"),
                )


class TestFallbackChainIntegration:

    @pytest.mark.asyncio
    async def test_primary_gemini_fallback_claude(self) -> None:
        gemini = AsyncMock()
        gemini.ainvoke = AsyncMock(side_effect=LLMError("gemini rate limited"))
        claude = AsyncMock()
        claude.ainvoke = AsyncMock(return_value="claude response")

        chain = FallbackChain(providers=[gemini, claude])
        result = await chain.ainvoke("test")
        assert result == "claude response"
        gemini.ainvoke.assert_called_once()
        claude.ainvoke.assert_called_once()

    @pytest.mark.asyncio
    async def test_structured_fallback(self) -> None:
        gemini = AsyncMock()
        gemini.ainvoke_structured = AsyncMock(
            side_effect=LLMError("gemini down"),
        )
        claude = AsyncMock()
        claude.ainvoke_structured = AsyncMock(return_value={"result": "ok"})

        chain = FallbackChain(providers=[gemini, claude])
        result = await chain.ainvoke_structured("prompt", ["msg"])
        assert result == {"result": "ok"}


class TestClaudeConfigFields:

    def test_extraction_config_has_anthropic_fields(self) -> None:
        cfg = _config()
        assert hasattr(cfg, "anthropic_api_key")
        assert hasattr(cfg, "claude_model_name")

    def test_extraction_config_from_env_reads_anthropic_key(self) -> None:
        env = {
            "GOOGLE_API_KEY": "g-key",
            "ANTHROPIC_API_KEY": "a-key",
            "CLAUDE_MODEL": "claude-sonnet-4-20250514",
        }
        with patch.dict("os.environ", env):
            cfg = ExtractionConfig.from_env()
        assert cfg.anthropic_api_key == "a-key"
        assert cfg.claude_model_name == "claude-sonnet-4-20250514"
