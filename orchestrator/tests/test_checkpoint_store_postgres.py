from unittest.mock import MagicMock, patch

from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.postgres import PostgresSaver

from orchestrator.app.checkpoint_store import (
    CheckpointStoreConfig,
    _create_postgres_checkpointer,
    close_checkpointer,
    get_checkpointer,
    init_checkpointer,
)


def test_postgres_checkpointer_returns_postgres_saver():
    with patch("orchestrator.app.checkpoint_store.psycopg") as mock_psycopg, \
         patch.object(PostgresSaver, "setup"):
        mock_psycopg.connect.return_value = MagicMock()
        cp = _create_postgres_checkpointer("postgresql://u:p@localhost:5432/db")
    assert isinstance(cp, PostgresSaver)
    assert not isinstance(cp, MemorySaver)


def test_init_with_postgres_backend_produces_non_memory_saver():
    env = {
        "CHECKPOINT_BACKEND": "postgres",
        "CHECKPOINT_POSTGRES_DSN": "postgresql://u:p@localhost:5432/db",
    }
    with patch.dict("os.environ", env, clear=False), \
         patch("orchestrator.app.checkpoint_store.psycopg") as mock_psycopg, \
         patch.object(PostgresSaver, "setup"):
        mock_psycopg.connect.return_value = MagicMock()
        init_checkpointer()
        cp = get_checkpointer()
        assert not isinstance(cp, MemorySaver)
        close_checkpointer()


def test_init_with_memory_backend_returns_memory_saver():
    env = {"CHECKPOINT_BACKEND": "memory", "CHECKPOINT_POSTGRES_DSN": ""}
    with patch.dict("os.environ", env, clear=False):
        init_checkpointer()
        cp = get_checkpointer()
        assert isinstance(cp, MemorySaver)
        close_checkpointer()


def test_close_checkpointer_clears_state():
    init_checkpointer()
    close_checkpointer()
    try:
        get_checkpointer()
        assert False, "Should have raised RuntimeError"
    except RuntimeError:
        pass


def test_config_from_env_defaults():
    with patch.dict("os.environ", {}, clear=False):
        cfg = CheckpointStoreConfig.from_env()
    assert cfg.backend == "memory"
    assert cfg.postgres_dsn == ""


def test_postgres_import_error_gives_helpful_message():
    with patch.dict("sys.modules", {"langgraph.checkpoint.postgres": None}):
        try:
            _create_postgres_checkpointer("postgresql://localhost/db")
            assert False, "Should have raised ImportError"
        except ImportError as exc:
            assert "langgraph-checkpoint-postgres" in str(exc)
