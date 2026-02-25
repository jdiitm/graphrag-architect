from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from langgraph.checkpoint.memory import MemorySaver

from orchestrator.app.checkpoint_store import (
    CheckpointStoreConfig,
    _create_postgres_checkpointer,
    close_checkpointer,
    get_checkpointer,
    init_checkpointer,
)


@pytest.mark.asyncio
async def test_postgres_checkpointer_returns_async_saver():
    mock_conn = AsyncMock()
    mock_saver_cls = MagicMock()
    mock_saver_instance = AsyncMock()
    mock_saver_cls.return_value = mock_saver_instance

    with patch("orchestrator.app.checkpoint_store.psycopg") as mock_psycopg, \
         patch(
             "langgraph.checkpoint.postgres.aio.AsyncPostgresSaver",
             mock_saver_cls,
         ):
        mock_psycopg.AsyncConnection.connect = AsyncMock(return_value=mock_conn)
        cp = await _create_postgres_checkpointer("postgresql://u:p@localhost:5432/db")

    assert cp is mock_saver_instance
    mock_saver_instance.setup.assert_awaited_once()
    assert not isinstance(cp, MemorySaver)


@pytest.mark.asyncio
async def test_init_with_postgres_backend_produces_non_memory_saver():
    env = {
        "CHECKPOINT_BACKEND": "postgres",
        "CHECKPOINT_POSTGRES_DSN": "postgresql://u:p@localhost:5432/db",
    }
    mock_conn = AsyncMock()
    mock_saver_cls = MagicMock()
    mock_saver_instance = AsyncMock()
    mock_saver_cls.return_value = mock_saver_instance

    with patch.dict("os.environ", env, clear=False), \
         patch("orchestrator.app.checkpoint_store.psycopg") as mock_psycopg, \
         patch(
             "langgraph.checkpoint.postgres.aio.AsyncPostgresSaver",
             mock_saver_cls,
         ):
        mock_psycopg.AsyncConnection.connect = AsyncMock(return_value=mock_conn)
        await init_checkpointer()
        cp = get_checkpointer()
        assert not isinstance(cp, MemorySaver)
        await close_checkpointer()


@pytest.mark.asyncio
async def test_init_with_memory_backend_returns_memory_saver():
    env = {"CHECKPOINT_BACKEND": "memory", "CHECKPOINT_POSTGRES_DSN": ""}
    with patch.dict("os.environ", env, clear=False):
        await init_checkpointer()
        cp = get_checkpointer()
        assert isinstance(cp, MemorySaver)
        await close_checkpointer()


@pytest.mark.asyncio
async def test_close_checkpointer_clears_state():
    await init_checkpointer()
    await close_checkpointer()
    with pytest.raises(RuntimeError):
        get_checkpointer()


@pytest.mark.asyncio
async def test_close_checkpointer_closes_connection():
    mock_conn = AsyncMock()
    mock_saver_cls = MagicMock()
    mock_saver_instance = AsyncMock()
    mock_saver_cls.return_value = mock_saver_instance

    env = {
        "CHECKPOINT_BACKEND": "postgres",
        "CHECKPOINT_POSTGRES_DSN": "postgresql://u:p@localhost:5432/db",
    }
    with patch.dict("os.environ", env, clear=False), \
         patch("orchestrator.app.checkpoint_store.psycopg") as mock_psycopg, \
         patch(
             "langgraph.checkpoint.postgres.aio.AsyncPostgresSaver",
             mock_saver_cls,
         ):
        mock_psycopg.AsyncConnection.connect = AsyncMock(return_value=mock_conn)
        await init_checkpointer()
    await close_checkpointer()
    mock_conn.close.assert_awaited_once()


def test_config_from_env_defaults():
    with patch.dict("os.environ", {}, clear=False):
        cfg = CheckpointStoreConfig.from_env()
    assert cfg.backend == "memory"
    assert cfg.postgres_dsn == ""


@pytest.mark.asyncio
async def test_postgres_import_error_gives_helpful_message():
    with patch.dict("sys.modules", {"langgraph.checkpoint.postgres.aio": None}):
        with pytest.raises(ImportError, match="langgraph-checkpoint-postgres"):
            await _create_postgres_checkpointer("postgresql://localhost/db")
