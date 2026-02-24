import os
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.query_models import QueryComplexity


@pytest.fixture(autouse=True)
def _reset_graph_builder_container() -> None:
    import orchestrator.app.graph_builder as gb
    gb.set_container(None)


@pytest.fixture(autouse=True)
def _default_auth_open_for_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    if "AUTH_REQUIRE_TOKENS" not in os.environ:
        monkeypatch.setenv("AUTH_REQUIRE_TOKENS", "false")


@pytest.fixture(autouse=True)
def _default_query_timeout():
    with patch(
        "orchestrator.app.query_engine._get_query_timeout",
        return_value=30.0,
    ):
        yield


@pytest.fixture(autouse=True)
def _reset_subgraph_cache():
    from orchestrator.app.query_engine import _SUBGRAPH_CACHE
    _SUBGRAPH_CACHE.invalidate_all()
    yield
    _SUBGRAPH_CACHE.invalidate_all()


@pytest.fixture
def base_query_state():
    return {
        "query": "",
        "max_results": 10,
        "complexity": QueryComplexity.ENTITY_LOOKUP,
        "retrieval_path": "",
        "candidates": [],
        "cypher_query": "",
        "cypher_results": [],
        "iteration_count": 0,
        "answer": "",
        "sources": [],
        "authorization": "",
    }


@pytest.fixture
def base_ingestion_state():
    return {
        "directory_path": "",
        "raw_files": [],
        "extracted_nodes": [],
        "extraction_errors": [],
        "validation_retries": 0,
        "commit_status": "",
        "extraction_checkpoint": {},
        "skipped_files": [],
    }


def mock_neo4j_driver_with_session(mock_session):
    mock_driver = MagicMock()
    mock_driver.session.return_value = mock_session
    mock_driver.close = AsyncMock()
    return mock_driver


def mock_async_session(execute_write_side_effect=None):
    mock_session = AsyncMock()
    if execute_write_side_effect:
        mock_session.execute_write = AsyncMock(
            side_effect=execute_write_side_effect
        )
    mock_driver = MagicMock()

    @asynccontextmanager
    async def session_ctx(**kwargs):
        yield mock_session

    mock_driver.session = session_ctx
    mock_driver.close = AsyncMock()
    return mock_driver, mock_session
