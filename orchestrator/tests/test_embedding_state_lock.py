"""Tests for _EMBEDDING_STATE race protection."""
import threading

from orchestrator.app.query_engine import _EMBEDDING_STATE_LOCK


class TestEmbeddingStateLock:
    def test_embedding_state_lock_exists(self) -> None:
        assert isinstance(_EMBEDDING_STATE_LOCK, type(threading.Lock()))
