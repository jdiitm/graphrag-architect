from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.query_engine import (
    _raw_llm_synthesize_stream,
    _llm_synthesize,
)


class _FakeLLMError(Exception):
    pass


@pytest.mark.asyncio
class TestStreamingDegradationContract:

    async def test_stream_error_yields_valid_json(self) -> None:
        mock_llm = MagicMock()

        async def _exploding_stream(*args, **kwargs):
            raise _FakeLLMError("down")

        mock_llm.astream = _exploding_stream

        with patch(
            "orchestrator.app.query_engine.LLMError", _FakeLLMError,
        ), patch(
            "orchestrator.app.query_engine._prompt_guardrails_enabled",
            return_value=False,
        ), patch(
            "orchestrator.app.query_engine.sanitize_query_input",
            side_effect=lambda q: q,
        ), patch(
            "orchestrator.app.query_engine.format_context_for_prompt",
        ) as mock_format, patch(
            "orchestrator.app.query_engine.ExtractionConfig",
        ) as mock_cfg, patch(
            "langchain_google_genai.ChatGoogleGenerativeAI",
            return_value=mock_llm,
        ):
            mock_cfg.from_env.return_value = MagicMock(
                model_name="gemini-pro", google_api_key="fake",
            )
            mock_format.return_value = MagicMock(
                content="ctx", delimiter="DATA",
            )

            chunks: list[str] = []
            async for chunk in _raw_llm_synthesize_stream(
                "test query", [{"node": "svc"}],
            ):
                chunks.append(chunk)

        assert len(chunks) >= 1
        error_payload = json.loads(chunks[-1])
        assert error_payload["type"] == "error"
        assert "code" in error_payload
        assert "message" in error_payload

    async def test_stream_error_never_yields_raw_apology(self) -> None:
        mock_llm = MagicMock()

        async def _exploding_stream(*args, **kwargs):
            raise _FakeLLMError("fail")

        mock_llm.astream = _exploding_stream

        with patch(
            "orchestrator.app.query_engine.LLMError", _FakeLLMError,
        ), patch(
            "orchestrator.app.query_engine._prompt_guardrails_enabled",
            return_value=False,
        ), patch(
            "orchestrator.app.query_engine.sanitize_query_input",
            side_effect=lambda q: q,
        ), patch(
            "orchestrator.app.query_engine.format_context_for_prompt",
        ) as mock_format, patch(
            "orchestrator.app.query_engine.ExtractionConfig",
        ) as mock_cfg, patch(
            "langchain_google_genai.ChatGoogleGenerativeAI",
            return_value=mock_llm,
        ):
            mock_cfg.from_env.return_value = MagicMock(
                model_name="gemini-pro", google_api_key="fake",
            )
            mock_format.return_value = MagicMock(
                content="ctx", delimiter="DATA",
            )

            chunks: list[str] = []
            async for chunk in _raw_llm_synthesize_stream("q", [{}]):
                chunks.append(chunk)

        full_output = "".join(chunks)
        assert "temporarily unavailable" not in full_output, (
            "Stream must not yield raw apology strings; "
            "use structured JSON error frames"
        )


@pytest.mark.asyncio
class TestNonStreamingDegradationContract:

    async def test_synthesize_returns_structured_degradation(self) -> None:
        from orchestrator.app.circuit_breaker import CircuitOpenError

        with patch(
            "orchestrator.app.query_engine._CB_LLM_GLOBAL",
        ) as mock_cb:
            mock_cb.call = AsyncMock(side_effect=CircuitOpenError("open"))

            result = await _llm_synthesize("test", [{}], tenant_id="t1")

        parsed = json.loads(result)
        assert parsed["type"] == "error"
        assert parsed["code"] == "CIRCUIT_OPEN"
        assert "message" in parsed
