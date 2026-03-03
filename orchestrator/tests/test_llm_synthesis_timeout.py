import asyncio
import json
import os
from unittest.mock import AsyncMock, patch

import pytest


class TestGetSynthesisTimeout:
    def test_synthesis_timeout_default_60s(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            from orchestrator.app.query_engine import _get_synthesis_timeout

            assert _get_synthesis_timeout() == 60.0

    def test_synthesis_timeout_configurable(self) -> None:
        with patch.dict(
            os.environ, {"LLM_SYNTHESIS_TIMEOUT_SECONDS": "10"}
        ):
            from orchestrator.app.query_engine import _get_synthesis_timeout

            assert _get_synthesis_timeout() == 10.0


class TestLLMSynthesisTimeout:
    @pytest.mark.asyncio
    async def test_synthesis_times_out_after_configured_seconds(self) -> None:
        from orchestrator.app.query_engine import _raw_llm_synthesize

        async def _hang(*_args: object, **_kwargs: object) -> str:
            await asyncio.sleep(999)
            return "never"

        mock_provider = AsyncMock()
        mock_provider.ainvoke_messages = _hang

        with (
            patch(
                "orchestrator.app.query_engine._build_synthesis_provider",
                return_value=mock_provider,
            ),
            patch(
                "orchestrator.app.query_engine._get_synthesis_timeout",
                return_value=0.1,
            ),
            patch(
                "orchestrator.app.query_engine._prompt_guardrails_enabled",
                return_value=False,
            ),
        ):
            with pytest.raises(asyncio.TimeoutError):
                await _raw_llm_synthesize("test query", [{"key": "val"}])

    @pytest.mark.asyncio
    async def test_stream_synthesis_times_out(self) -> None:
        from orchestrator.app.query_engine import _raw_llm_synthesize_stream

        async def _hanging_stream(*_args: object, **_kwargs: object):  # type: ignore[no-untyped-def]
            await asyncio.sleep(999)
            yield "never"

        mock_provider = AsyncMock()
        mock_provider.astream = _hanging_stream

        with (
            patch(
                "orchestrator.app.query_engine._build_synthesis_provider",
                return_value=mock_provider,
            ),
            patch(
                "orchestrator.app.query_engine._get_synthesis_timeout",
                return_value=0.1,
            ),
            patch(
                "orchestrator.app.query_engine._prompt_guardrails_enabled",
                return_value=False,
            ),
        ):
            chunks = []
            async for chunk in _raw_llm_synthesize_stream(
                "test query", [{"key": "val"}]
            ):
                chunks.append(chunk)

            assert len(chunks) == 1
            payload = json.loads(chunks[0])
            assert payload["type"] == "error"
            assert payload["code"] == "SYNTHESIS_TIMEOUT"
