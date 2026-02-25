import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.agentic_traversal import (
    MAX_NODE_DEGREE,
    execute_hop,
)


def _mock_driver(degree_result, hop_results=None):
    driver = AsyncMock()
    session = AsyncMock()

    call_count = {"n": 0}

    async def _execute_read(tx_fn, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return degree_result
        return hop_results or []

    session.execute_read = _execute_read
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    driver.session = MagicMock(return_value=session)
    return driver


def test_normal_node_expanded():
    loop = asyncio.new_event_loop()

    async def _run():
        degree_data = [{"degree": 10}]
        hop_data = [{"target_id": "b", "target_name": "b", "rel_type": "CALLS"}]
        driver = _mock_driver(degree_data, hop_data)
        results = await execute_hop(
            driver=driver, source_id="a", tenant_id="t1",
            acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
        )
        assert len(results) == 1
        assert results[0]["target_id"] == "b"

    loop.run_until_complete(_run())
    loop.close()


def test_supernode_returns_empty():
    loop = asyncio.new_event_loop()

    async def _run():
        degree_data = [{"degree": MAX_NODE_DEGREE + 1}]
        driver = _mock_driver(degree_data)
        results = await execute_hop(
            driver=driver, source_id="hub", tenant_id="t1",
            acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
        )
        assert results == []

    loop.run_until_complete(_run())
    loop.close()


def test_exactly_at_threshold_expanded():
    loop = asyncio.new_event_loop()

    async def _run():
        degree_data = [{"degree": MAX_NODE_DEGREE}]
        hop_data = [{"target_id": "x"}]
        driver = _mock_driver(degree_data, hop_data)
        results = await execute_hop(
            driver=driver, source_id="a", tenant_id="t1",
            acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
        )
        assert len(results) == 1

    loop.run_until_complete(_run())
    loop.close()
