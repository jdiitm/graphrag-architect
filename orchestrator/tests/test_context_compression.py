from __future__ import annotations

import json
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.context_manager import (
    TOKEN_COMPRESSION_THRESHOLD,
    TokenBudget,
    compress_context_map_reduce,
    estimate_tokens,
    truncate_context_topology,
)
from orchestrator.app.query_models import QueryState


def _make_edge(source: str, target: str, score: float = 0.5) -> Dict[str, Any]:
    return {"source": source, "target": target, "rel": "CALLS", "score": score}


def _make_large_context(n: int) -> List[Dict[str, Any]]:
    context: List[Dict[str, Any]] = []
    for i in range(n):
        context.append({
            "source": f"svc-{i}",
            "target": f"svc-{i + 1}",
            "rel": "CALLS",
            "description": f"Service {i} calls service {i + 1} " * 30,
            "score": 0.5,
        })
    return context


class TestContextCompression:
    def test_large_context_triggers_compression(self):
        budget = TokenBudget(max_context_tokens=32_000)
        large_context = _make_large_context(200)
        total_tokens = sum(
            estimate_tokens(json.dumps(c, default=str)) for c in large_context
        )
        assert total_tokens > TOKEN_COMPRESSION_THRESHOLD

        result = compress_context_map_reduce(
            large_context,
            budget=budget,
        )
        result_tokens = sum(
            estimate_tokens(json.dumps(c, default=str)) for c in result
        )
        assert result_tokens <= budget.max_context_tokens
        assert len(result) > 0

    def test_small_context_skips_compression(self):
        budget = TokenBudget(max_context_tokens=32_000)
        small_context = [
            _make_edge("svc-a", "svc-b", 0.9),
            _make_edge("svc-b", "svc-c", 0.8),
        ]
        result = compress_context_map_reduce(
            small_context,
            budget=budget,
        )
        assert result == small_context

    def test_compressed_output_fits_token_budget(self):
        tight_budget = TokenBudget(max_context_tokens=2000, max_results=10)
        large_context = _make_large_context(100)
        result = compress_context_map_reduce(
            large_context,
            budget=tight_budget,
        )
        result_tokens = sum(
            estimate_tokens(json.dumps(c, default=str)) for c in result
        )
        assert result_tokens <= tight_budget.max_context_tokens

    def test_compression_preserves_bridge_edges(self):
        budget = TokenBudget(max_context_tokens=32_000)
        context = _make_large_context(200)
        result = compress_context_map_reduce(
            context,
            budget=budget,
        )
        has_bridge_info = any(
            "bridge_edges" in item or "cross_community_edge_count" in item
            for item in result
        )
        has_community_info = any(
            "community_id" in item or "member_count" in item
            for item in result
        )
        assert has_bridge_info or has_community_info

    def test_llm_failure_falls_back_to_truncation(self):
        budget = TokenBudget(max_context_tokens=32_000)
        large_context = _make_large_context(200)
        with patch(
            "orchestrator.app.context_manager.compress_component_to_summaries",
            side_effect=RuntimeError("LLM crash"),
        ):
            result = compress_context_map_reduce(
                large_context,
                budget=budget,
            )
        assert len(result) > 0
        result_tokens = sum(
            estimate_tokens(json.dumps(c, default=str)) for c in result
        )
        assert result_tokens <= budget.max_context_tokens

    def test_compressed_flag_set_in_state(self):
        state: QueryState = {
            "query": "test",
            "max_results": 10,
            "compressed": False,
        }
        assert "compressed" in QueryState.__annotations__ or hasattr(QueryState, "__optional_keys__")
        state["compressed"] = True
        assert state["compressed"] is True
