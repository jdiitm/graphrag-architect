from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from orchestrator.app.checkpoint_store import (
    CheckpointStoreConfig,
    close_checkpointer,
    get_checkpointer,
    init_checkpointer,
)


class TestCheckpointStoreConfig:
    def test_default_values(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            cfg = CheckpointStoreConfig.from_env()
        assert cfg.backend == "memory"
        assert cfg.postgres_dsn == ""

    def test_postgres_backend(self) -> None:
        env = {
            "CHECKPOINT_BACKEND": "postgres",
            "CHECKPOINT_POSTGRES_DSN": "postgresql://user:pass@host:5432/db",
        }
        with patch.dict("os.environ", env, clear=True):
            cfg = CheckpointStoreConfig.from_env()
        assert cfg.backend == "postgres"
        assert cfg.postgres_dsn == "postgresql://user:pass@host:5432/db"


class TestCheckpointerLifecycle:
    def setup_method(self) -> None:
        close_checkpointer()

    def teardown_method(self) -> None:
        close_checkpointer()

    def test_get_before_init_raises(self) -> None:
        with pytest.raises(RuntimeError, match="not initialized"):
            get_checkpointer()

    def test_init_creates_memory_checkpointer(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            init_checkpointer()
        cp = get_checkpointer()
        assert cp is not None

    def test_close_resets_state(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            init_checkpointer()
        close_checkpointer()
        with pytest.raises(RuntimeError, match="not initialized"):
            get_checkpointer()

    def test_double_init_replaces_checkpointer(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            init_checkpointer()
            first = get_checkpointer()
            init_checkpointer()
            second = get_checkpointer()
        assert first is not second

    def test_memory_checkpointer_has_correct_type(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            init_checkpointer()
        cp = get_checkpointer()
        from langgraph.checkpoint.memory import MemorySaver
        assert isinstance(cp, MemorySaver)
