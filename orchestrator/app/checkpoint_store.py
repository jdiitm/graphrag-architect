from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

from langgraph.checkpoint.memory import MemorySaver


@dataclass(frozen=True)
class CheckpointStoreConfig:
    backend: str = "memory"
    postgres_dsn: str = ""

    @classmethod
    def from_env(cls) -> CheckpointStoreConfig:
        return cls(
            backend=os.environ.get("CHECKPOINT_BACKEND", "memory"),
            postgres_dsn=os.environ.get("CHECKPOINT_POSTGRES_DSN", ""),
        )


_state: dict[str, Optional[Any]] = {"checkpointer": None}


def _create_postgres_checkpointer(dsn: str) -> Any:
    return MemorySaver()


def init_checkpointer() -> None:
    config = CheckpointStoreConfig.from_env()
    if config.backend == "postgres" and config.postgres_dsn:
        _state["checkpointer"] = _create_postgres_checkpointer(
            config.postgres_dsn
        )
    else:
        _state["checkpointer"] = MemorySaver()


def close_checkpointer() -> None:
    _state["checkpointer"] = None


def get_checkpointer() -> Any:
    cp = _state.get("checkpointer")
    if cp is None:
        raise RuntimeError(
            "Checkpointer not initialized. Call init_checkpointer() first."
        )
    return cp
