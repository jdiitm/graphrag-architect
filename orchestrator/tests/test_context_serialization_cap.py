"""Tests for context serialization size cap."""
from orchestrator.app.query_engine import _serialize_context_for_classification


class TestContextSerializationCap:
    def test_small_context_unchanged(self) -> None:
        context = [{"text": "hello world", "score": "0.9"}]
        result = _serialize_context_for_classification(context)
        assert "hello world" in result
        assert "0.9" in result

    def test_large_context_truncated(self) -> None:
        huge_value = "x" * 50_000
        context = [{"text": huge_value}]
        result = _serialize_context_for_classification(context)
        assert len(result) <= 20_000

    def test_empty_context_returns_empty(self) -> None:
        result = _serialize_context_for_classification([])
        assert result == ""

    def test_multiple_items_within_cap(self) -> None:
        context = [{"v": "a" * 5_000}, {"v": "b" * 5_000}]
        result = _serialize_context_for_classification(context)
        assert len(result) <= 20_000
