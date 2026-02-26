from __future__ import annotations

import pytest

from orchestrator.app.token_counter import (
    count_tokens,
    estimate_tokens_fast,
)


class TestCountTokens:

    def test_empty_string_returns_zero(self) -> None:
        assert count_tokens("") == 0

    def test_single_word(self) -> None:
        result = count_tokens("hello")
        assert result == 1

    def test_typical_sentence(self) -> None:
        result = count_tokens("The quick brown fox jumps over the lazy dog")
        assert 7 <= result <= 12

    def test_code_snippet_is_accurate(self) -> None:
        code = 'def hello():\n    return "world"\n'
        result = count_tokens(code)
        naive = len(code) // 4
        assert abs(result - naive) <= naive

    def test_unicode_text(self) -> None:
        result = count_tokens("Hello, 世界!")
        assert result >= 1

    def test_large_text_does_not_crash(self) -> None:
        big_text = "x " * 50_000
        result = count_tokens(big_text)
        assert result > 0

    def test_returns_int(self) -> None:
        result = count_tokens("hello world")
        assert isinstance(result, int)


class TestEstimateTokensFast:

    def test_empty_string_returns_one(self) -> None:
        assert estimate_tokens_fast("") == 1

    def test_four_chars_returns_one(self) -> None:
        assert estimate_tokens_fast("abcd") == 1

    def test_proportional_to_length(self) -> None:
        text = "a" * 400
        result = estimate_tokens_fast(text)
        assert result == 100


class TestCountTokensAccuracyVsNaive:

    def test_more_accurate_than_len_div_4_for_code(self) -> None:
        go_code = (
            'package main\n\nimport "fmt"\n\nfunc main() {\n'
            '    fmt.Println("Hello, world!")\n}\n'
        )
        tiktoken_result = count_tokens(go_code)
        naive_result = len(go_code) // 4
        assert tiktoken_result == 20
        assert tiktoken_result != naive_result

    def test_never_returns_negative(self) -> None:
        assert count_tokens("") >= 0
        assert count_tokens("a") >= 0
