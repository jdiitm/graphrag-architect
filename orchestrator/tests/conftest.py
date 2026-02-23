from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.query_models import QueryComplexity


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
