from __future__ import annotations

import pytest

from orchestrator.app.config import ASTResourceGuard


class TestASTResourceGuard:
    def test_defaults(self) -> None:
        guard = ASTResourceGuard()
        assert guard.max_file_size_bytes == 2_097_152
        assert guard.per_file_timeout_seconds == 30.0
        assert guard.pool_exhaustion_timeout == 60.0

    def test_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AST_MAX_FILE_SIZE_BYTES", "1048576")
        monkeypatch.setenv("AST_PER_FILE_TIMEOUT", "15.0")
        monkeypatch.setenv("AST_POOL_EXHAUSTION_TIMEOUT", "45.0")
        guard = ASTResourceGuard.from_env()
        assert guard.max_file_size_bytes == 1_048_576
        assert guard.per_file_timeout_seconds == 15.0
        assert guard.pool_exhaustion_timeout == 45.0

    def test_frozen(self) -> None:
        guard = ASTResourceGuard()
        with pytest.raises(AttributeError):
            guard.max_file_size_bytes = 10

    def test_rejects_oversized_content(self) -> None:
        guard = ASTResourceGuard(max_file_size_bytes=100)
        oversized = "x" * 200
        assert guard.exceeds_file_size(oversized) is True

    def test_allows_small_content(self) -> None:
        guard = ASTResourceGuard(max_file_size_bytes=1000)
        small = "x" * 50
        assert guard.exceeds_file_size(small) is False
