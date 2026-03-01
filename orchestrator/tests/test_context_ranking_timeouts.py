import asyncio
import logging
import os
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.app.config import ContextRankingConfig


def _make_state(base_query_state, **overrides):
    return {**base_query_state, **overrides}


class TestContextRankingConfigDefaults:
    def test_rerank_timeout_default(self):
        config = ContextRankingConfig()
        assert config.rerank_timeout_seconds == 5.0

    def test_truncation_timeout_default(self):
        config = ContextRankingConfig()
        assert config.truncation_timeout_seconds == 3.0


class TestContextRankingConfigFromEnv:
    def test_reads_rerank_timeout_from_env(self):
        with patch.dict(os.environ, {"RERANK_TIMEOUT_SECONDS": "10.0"}):
            config = ContextRankingConfig.from_env()
        assert config.rerank_timeout_seconds == 10.0

    def test_reads_truncation_timeout_from_env(self):
        with patch.dict(os.environ, {"TRUNCATION_TIMEOUT_SECONDS": "7.5"}):
            config = ContextRankingConfig.from_env()
        assert config.truncation_timeout_seconds == 7.5

    def test_uses_defaults_when_env_unset(self):
        with patch.dict(os.environ, {}, clear=True):
            config = ContextRankingConfig.from_env()
        assert config.rerank_timeout_seconds == 5.0
        assert config.truncation_timeout_seconds == 3.0

    def test_custom_env_values_override_defaults(self):
        with patch.dict(os.environ, {
            "RERANK_TIMEOUT_SECONDS": "12.5",
            "TRUNCATION_TIMEOUT_SECONDS": "8.0",
        }):
            config = ContextRankingConfig.from_env()
        assert config.rerank_timeout_seconds == 12.5
        assert config.truncation_timeout_seconds == 8.0


class TestRerankTimeoutBehavior:
    @pytest.mark.asyncio
    async def test_normal_reranking_completes_within_timeout(
        self, base_query_state,
    ):
        from orchestrator.app.query_engine import _do_synthesize

        state = _make_state(
            base_query_state,
            candidates=[{"name": "svc-a", "score": 0.9}],
        )

        with patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="test answer",
        ):
            result = await _do_synthesize(state)

        assert result["answer"] == "test answer"
        assert len(result["sources"]) > 0

    @pytest.mark.asyncio
    async def test_rerank_timeout_falls_back_to_unranked_context(
        self, base_query_state,
    ):
        from orchestrator.app.query_engine import _do_synthesize

        original_candidates = [
            {"name": "svc-a", "score": 0.9},
            {"name": "svc-b", "score": 0.8},
        ]

        state = _make_state(
            base_query_state,
            candidates=original_candidates,
        )

        with patch(
            "orchestrator.app.query_engine._async_rerank_candidates",
            new_callable=AsyncMock,
            side_effect=asyncio.TimeoutError,
        ), patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="degraded answer",
        ) as mock_llm:
            result = await _do_synthesize(state)

        assert result["answer"] == "degraded answer"
        llm_context = mock_llm.call_args[0][1]
        context_names = {c.get("name") for c in llm_context}
        assert context_names == {"svc-a", "svc-b"}


class TestTruncationTimeoutBehavior:
    @pytest.mark.asyncio
    async def test_truncation_timeout_falls_back_to_ranked_context(
        self, base_query_state,
    ):
        from orchestrator.app.query_engine import _do_synthesize

        state = _make_state(
            base_query_state,
            candidates=[{"name": "svc-a", "score": 0.9}],
        )

        with patch(
            "orchestrator.app.query_engine.truncate_context_topology",
            side_effect=asyncio.TimeoutError,
        ), patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="truncation fallback answer",
        ):
            result = await _do_synthesize(state)

        assert result["answer"] == "truncation fallback answer"
        assert len(result["sources"]) > 0


class TestTimeoutDegradedSynthesis:
    @pytest.mark.asyncio
    async def test_synthesis_valid_with_degraded_rerank_context(
        self, base_query_state,
    ):
        from orchestrator.app.query_engine import _do_synthesize

        state = _make_state(
            base_query_state,
            candidates=[
                {"name": "auth-service", "score": 0.95},
                {"name": "user-service", "score": 0.80},
            ],
        )

        with patch(
            "orchestrator.app.query_engine._async_rerank_candidates",
            new_callable=AsyncMock,
            side_effect=asyncio.TimeoutError,
        ), patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="auth-service is a Go microservice",
        ):
            result = await _do_synthesize(state)

        assert isinstance(result["answer"], str)
        assert len(result["answer"]) > 0
        assert isinstance(result["sources"], list)
        assert len(result["sources"]) > 0


class TestTimeoutWarningLogs:
    @pytest.mark.asyncio
    async def test_warning_logged_on_rerank_timeout(
        self, base_query_state, caplog,
    ):
        from orchestrator.app.query_engine import _do_synthesize

        state = _make_state(
            base_query_state,
            candidates=[{"name": "svc-a", "score": 0.9}],
        )

        with patch(
            "orchestrator.app.query_engine._async_rerank_candidates",
            new_callable=AsyncMock,
            side_effect=asyncio.TimeoutError,
        ), patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="answer",
        ), caplog.at_level(logging.WARNING):
            await _do_synthesize(state)

        assert any("Reranking timed out" in msg for msg in caplog.messages), (
            f"Expected 'Reranking timed out' warning, got: {caplog.messages}"
        )

    @pytest.mark.asyncio
    async def test_warning_logged_on_truncation_timeout(
        self, base_query_state, caplog,
    ):
        from orchestrator.app.query_engine import _do_synthesize

        state = _make_state(
            base_query_state,
            candidates=[{"name": "svc-a", "score": 0.9}],
        )

        with patch(
            "orchestrator.app.query_engine.truncate_context_topology",
            side_effect=asyncio.TimeoutError,
        ), patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="answer",
        ), caplog.at_level(logging.WARNING):
            await _do_synthesize(state)

        assert any("Truncation timed out" in msg for msg in caplog.messages), (
            f"Expected 'Truncation timed out' warning, got: {caplog.messages}"
        )


class TestZeroTimeoutBackwardCompat:
    def test_zero_timeout_config_values(self):
        with patch.dict(os.environ, {
            "RERANK_TIMEOUT_SECONDS": "0",
            "TRUNCATION_TIMEOUT_SECONDS": "0",
        }):
            config = ContextRankingConfig.from_env()
        assert config.rerank_timeout_seconds == 0.0
        assert config.truncation_timeout_seconds == 0.0

    @pytest.mark.asyncio
    async def test_zero_timeout_does_not_trigger_timeout(
        self, base_query_state,
    ):
        from orchestrator.app.query_engine import _do_synthesize

        state = _make_state(
            base_query_state,
            candidates=[{"name": "svc-a", "score": 0.9}],
        )

        with patch.dict(os.environ, {
            "RERANK_TIMEOUT_SECONDS": "0",
            "TRUNCATION_TIMEOUT_SECONDS": "0",
        }), patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="answer with no timeout",
        ):
            result = await _do_synthesize(state)

        assert result["answer"] == "answer with no timeout"
        assert len(result["sources"]) > 0
