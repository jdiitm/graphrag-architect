import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml
from langchain_core.messages import HumanMessage, SystemMessage

from orchestrator.app.circuit_breaker import (
    CircuitOpenError,
)


class TestEmbeddingFallbackMetricExists:
    def test_embedding_fallback_total_counter_exists(self):
        from orchestrator.app.observability import EMBEDDING_FALLBACK_TOTAL

        assert EMBEDDING_FALLBACK_TOTAL is not None

    def test_counter_has_correct_name(self):
        from orchestrator.app.observability import EMBEDDING_FALLBACK_TOTAL

        assert EMBEDDING_FALLBACK_TOTAL._name == "embedding.fallback_total"


class TestEmbedQueryFallbackMetric:
    @pytest.mark.asyncio
    async def test_increments_on_circuit_open(self):
        from orchestrator.app.query_engine import _embed_query

        mock_global = AsyncMock()
        mock_global.call = AsyncMock(side_effect=CircuitOpenError("open"))

        with patch(
            "orchestrator.app.query_engine._CB_EMBEDDING_GLOBAL", mock_global,
        ), patch(
            "orchestrator.app.query_engine.EMBEDDING_FALLBACK_TOTAL",
        ) as mock_counter:
            result = await _embed_query("test query")

        assert result is None
        mock_counter.add.assert_called_once_with(1, {"reason": "circuit_open"})

    @pytest.mark.asyncio
    async def test_increments_on_generic_exception(self):
        from orchestrator.app.query_engine import _embed_query

        mock_global = AsyncMock()
        mock_global.call = AsyncMock(side_effect=RuntimeError("API error"))

        with patch(
            "orchestrator.app.query_engine._CB_EMBEDDING_GLOBAL", mock_global,
        ), patch(
            "orchestrator.app.query_engine.EMBEDDING_FALLBACK_TOTAL",
        ) as mock_counter:
            result = await _embed_query("test query")

        assert result is None
        mock_counter.add.assert_called_once_with(1, {"reason": "exception"})

    @pytest.mark.asyncio
    async def test_no_increment_on_success(self):
        from orchestrator.app.query_engine import _embed_query

        fake_embedding = [0.1] * 10
        mock_global = AsyncMock()
        mock_global.call = AsyncMock(return_value=fake_embedding)

        with patch(
            "orchestrator.app.query_engine._CB_EMBEDDING_GLOBAL", mock_global,
        ), patch(
            "orchestrator.app.query_engine.EMBEDDING_FALLBACK_TOTAL",
        ) as mock_counter:
            result = await _embed_query("test query")

        assert result == fake_embedding
        mock_counter.add.assert_not_called()


class TestStructuredLLMMessages:
    @pytest.mark.asyncio
    async def test_raw_llm_synthesize_uses_message_list(self):
        from orchestrator.app.query_engine import _raw_llm_synthesize

        mock_provider = MagicMock()
        mock_provider.ainvoke_messages = AsyncMock(return_value="Auth handles authentication.")

        with patch(
            "orchestrator.app.query_engine._build_synthesis_provider", return_value=mock_provider,
        ):
            await _raw_llm_synthesize("What is auth?", [{"name": "auth"}])

        call_args = mock_provider.ainvoke_messages.call_args[0][0]
        assert isinstance(call_args, list), (
            f"ainvoke_messages must receive a list of messages, got {type(call_args)}"
        )

    @pytest.mark.asyncio
    async def test_raw_llm_synthesize_has_system_message(self):
        from orchestrator.app.query_engine import _raw_llm_synthesize

        mock_provider = MagicMock()
        mock_provider.ainvoke_messages = AsyncMock(return_value="Auth handles authentication.")

        with patch(
            "orchestrator.app.query_engine._build_synthesis_provider", return_value=mock_provider,
        ):
            await _raw_llm_synthesize("What is auth?", [{"name": "auth"}])

        messages = mock_provider.ainvoke_messages.call_args[0][0]
        system_msgs = [m for m in messages if isinstance(m, SystemMessage)]
        assert len(system_msgs) == 1, (
            f"Expected exactly 1 SystemMessage, got {len(system_msgs)}"
        )

    @pytest.mark.asyncio
    async def test_raw_llm_synthesize_has_human_message(self):
        from orchestrator.app.query_engine import _raw_llm_synthesize

        mock_provider = MagicMock()
        mock_provider.ainvoke_messages = AsyncMock(return_value="Auth handles authentication.")

        with patch(
            "orchestrator.app.query_engine._build_synthesis_provider", return_value=mock_provider,
        ):
            await _raw_llm_synthesize("What is auth?", [{"name": "auth"}])

        messages = mock_provider.ainvoke_messages.call_args[0][0]
        human_msgs = [m for m in messages if isinstance(m, HumanMessage)]
        assert len(human_msgs) == 1, (
            f"Expected exactly 1 HumanMessage, got {len(human_msgs)}"
        )

    @pytest.mark.asyncio
    async def test_human_message_contains_sanitized_query(self):
        from orchestrator.app.query_engine import _raw_llm_synthesize

        mock_provider = MagicMock()
        mock_provider.ainvoke_messages = AsyncMock(return_value="Answer.")

        with patch(
            "orchestrator.app.query_engine._build_synthesis_provider", return_value=mock_provider,
        ):
            await _raw_llm_synthesize("What is auth?", [{"name": "auth"}])

        messages = mock_provider.ainvoke_messages.call_args[0][0]
        human_msg = next(m for m in messages if isinstance(m, HumanMessage))
        assert "<user_query>" in human_msg.content


class TestAlertingYamlEmbeddingRule:
    def test_alerting_yaml_contains_embedding_fallback_alert(self):
        alerting_path = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "infrastructure", "k8s", "alerting.yaml",
        )
        with open(alerting_path, encoding="utf-8") as fh:
            docs = yaml.safe_load(fh)

        all_alerts = []
        for group in docs["spec"]["groups"]:
            for rule in group.get("rules", []):
                if "alert" in rule:
                    all_alerts.append(rule["alert"])

        assert "EmbeddingFallbackRateHigh" in all_alerts, (
            f"Expected EmbeddingFallbackRateHigh alert, found: {all_alerts}"
        )

    def test_embedding_alert_references_correct_metric(self):
        alerting_path = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "infrastructure", "k8s", "alerting.yaml",
        )
        with open(alerting_path, encoding="utf-8") as fh:
            docs = yaml.safe_load(fh)

        for group in docs["spec"]["groups"]:
            for rule in group.get("rules", []):
                if rule.get("alert") == "EmbeddingFallbackRateHigh":
                    assert "embedding_fallback_total" in rule["expr"]
                    return

        pytest.fail("EmbeddingFallbackRateHigh rule not found")
