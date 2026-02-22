from __future__ import annotations

from typing import Optional

from neo4j import AsyncDriver, AsyncGraphDatabase

from orchestrator.app.config import Neo4jConfig

_state: dict[str, Optional[AsyncDriver]] = {"driver": None}


def init_driver() -> None:
    config = Neo4jConfig.from_env()
    _state["driver"] = AsyncGraphDatabase.driver(
        config.uri,
        auth=(config.username, config.password),
        max_transaction_retry_time=config.query_timeout,
    )


async def close_driver() -> None:
    driver = _state.get("driver")
    if driver is not None:
        await driver.close()
        _state["driver"] = None


def get_driver() -> AsyncDriver:
    driver = _state.get("driver")
    if driver is None:
        raise RuntimeError(
            "Neo4j driver not initialized. Call init_driver() first."
        )
    return driver
