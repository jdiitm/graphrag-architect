from __future__ import annotations

import ast
import json
from typing import Any, Dict, List

import pytest

from orchestrator.app.context_manager import (
    _truncate_value,
    format_context_for_prompt,
)


class TestTruncateListValue:

    def test_long_list_produces_valid_python_literal(self) -> None:
        long_list = [f"namespace-{i}" for i in range(50)]
        result = _truncate_value(long_list, max_chars=80)
        assert result.startswith("[")
        assert result.endswith("]")
        parsed = ast.literal_eval(result)
        assert isinstance(parsed, list)

    def test_long_list_includes_summary_element(self) -> None:
        long_list = [f"ns-{i}" for i in range(50)]
        result = _truncate_value(long_list, max_chars=60)
        parsed = ast.literal_eval(result)
        summary_elements = [e for e in parsed if "more" in str(e).lower()]
        assert len(summary_elements) >= 1

    def test_long_list_has_fewer_elements_than_original(self) -> None:
        long_list = [f"item-{i}" for i in range(100)]
        result = _truncate_value(long_list, max_chars=80)
        parsed = ast.literal_eval(result)
        assert len(parsed) < len(long_list)

    def test_short_list_unchanged(self) -> None:
        short_list = ["a", "b", "c"]
        result = _truncate_value(short_list, max_chars=500)
        assert "a" in result
        assert "b" in result
        assert "c" in result


class TestTruncateDictValue:

    def test_long_dict_produces_valid_structure(self) -> None:
        big_dict = {f"key_{i}": f"value_{i}" for i in range(50)}
        result = _truncate_value(big_dict, max_chars=100)
        assert result.startswith("{")
        assert result.endswith("}")

    def test_long_dict_includes_summary_key(self) -> None:
        big_dict = {f"k{i}": f"v{i}" for i in range(50)}
        result = _truncate_value(big_dict, max_chars=80)
        assert "more" in result.lower()

    def test_short_dict_unchanged(self) -> None:
        small_dict = {"a": 1, "b": 2}
        result = _truncate_value(small_dict, max_chars=500)
        assert "a" in result
        assert "b" in result


class TestTruncateStringValue:

    def test_long_string_truncates_at_word_boundary(self) -> None:
        long_text = "the quick brown fox jumps over the lazy dog " * 20
        result = _truncate_value(long_text, max_chars=50)
        assert len(result) <= 60
        assert result.endswith("...")
        prefix = result[:-3]
        assert not prefix[-1].isalnum() or prefix.endswith(
            tuple(long_text.split())
        )

    def test_short_string_unchanged(self) -> None:
        text = "hello world"
        result = _truncate_value(text, max_chars=500)
        assert result == "hello world"


class TestTruncateEdgeCases:

    def test_numeric_value_unchanged(self) -> None:
        assert _truncate_value(42, max_chars=500) == "42"
        assert _truncate_value(3.14, max_chars=500) == "3.14"

    def test_boolean_value_unchanged(self) -> None:
        assert _truncate_value(True, max_chars=500) == "True"

    def test_none_value_unchanged(self) -> None:
        assert _truncate_value(None, max_chars=500) == "None"

    def test_empty_list(self) -> None:
        result = _truncate_value([], max_chars=500)
        assert result == "[]"

    def test_empty_dict(self) -> None:
        result = _truncate_value({}, max_chars=500)
        assert result == "{}"

    def test_empty_string(self) -> None:
        result = _truncate_value("", max_chars=500)
        assert result == ""


class TestFormatContextNoBrokenBrackets:

    def test_oversized_list_values_produce_balanced_output(self) -> None:
        context: List[Dict[str, Any]] = [
            {
                "service": "auth-service",
                "namespace_acl": [f"ns-{i}" for i in range(100)],
                "dependencies": [f"dep-{i}" for i in range(100)],
            }
        ]
        block = format_context_for_prompt(context, max_chars_per_value=80)
        content = block.content
        assert content.count("[") == content.count("]")

    def test_oversized_dict_values_produce_balanced_output(self) -> None:
        context: List[Dict[str, Any]] = [
            {
                "service": "api-gateway",
                "metadata": {f"k{i}": f"v{i}" for i in range(100)},
            }
        ]
        block = format_context_for_prompt(context, max_chars_per_value=80)
        content = block.content
        assert content.count("{") == content.count("}")

    def test_mixed_value_types_no_syntax_errors(self) -> None:
        context: List[Dict[str, Any]] = [
            {
                "id": "svc-1",
                "name": "order-service",
                "acl": [f"ns-{i}" for i in range(50)],
                "config": {f"k{i}": f"v{i}" for i in range(50)},
                "description": "a]b[c{d}e" * 50,
                "replicas": 3,
            }
        ]
        block = format_context_for_prompt(context, max_chars_per_value=60)
        assert block.content
        assert block.delimiter
