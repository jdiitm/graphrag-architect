from __future__ import annotations

from typing import Optional

from neo4j import AsyncDriver, AsyncGraphDatabase

from orchestrator.app.config import Neo4jConfig

_state: dict[str, Optional[AsyncDriver | float | str]] = {
    "driver": None,
    "query_timeout": None,
    "database": None,
}


def init_driver() -> None:
    config = Neo4jConfig.from_env()
    _state["driver"] = AsyncGraphDatabase.driver(
        config.uri,
        auth=(config.username, config.password),
        max_transaction_retry_time=config.query_timeout,
    )
    _state["query_timeout"] = config.query_timeout
    _state["database"] = config.database


async def close_driver() -> None:
    driver = _state.get("driver")
    if driver is not None:
        await driver.close()
        _state["driver"] = None


def get_query_timeout() -> float:
    timeout = _state.get("query_timeout")
    if timeout is None:
        raise RuntimeError(
            "Neo4j driver not initialized. Call init_driver() first."
        )
    return timeout


def get_database() -> str:
    database = _state.get("database")
    if database is None:
        raise RuntimeError(
            "Neo4j driver not initialized. Call init_driver() first."
        )
    return database


def get_driver() -> AsyncDriver:
    driver = _state.get("driver")
    if driver is None:
        raise RuntimeError(
            "Neo4j driver not initialized. Call init_driver() first."
        )
    return driver
