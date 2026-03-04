from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.neo4j_client import GraphRepository


def _mock_driver() -> AsyncMock:
    driver = AsyncMock()
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    driver.session = MagicMock(return_value=session)
    return driver


@pytest.mark.asyncio
async def test_reap_tombstoned_edges_requires_tenant_id() -> None:
    repo = GraphRepository(_mock_driver())
    with pytest.raises(ValueError, match="tenant_id"):
        await repo.reap_tombstoned_edges(tenant_id="")
