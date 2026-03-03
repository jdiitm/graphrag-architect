from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from orchestrator.app.community_summary import (
    generate_summary_with_llm,
    tenant_scoped_community_detection_stub,
)


@pytest.mark.asyncio
async def test_generate_summary_with_llm_uses_provider_output() -> None:
    provider = AsyncMock()
    provider.ainvoke = AsyncMock(return_value="Auth cluster fronts user login flows.")
    summary = await generate_summary_with_llm(
        provider=provider,
        tenant_id="t1",
        community_id="c1",
        member_names=["auth", "session", "token"],
        member_types=["Service", "Service", "Service"],
    )
    assert "Auth cluster" in summary
    provider.ainvoke.assert_awaited_once()


@pytest.mark.asyncio
async def test_generate_summary_with_llm_falls_back_on_provider_error() -> None:
    provider = AsyncMock()
    provider.ainvoke = AsyncMock(side_effect=RuntimeError("provider down"))
    summary = await generate_summary_with_llm(
        provider=provider,
        tenant_id="t1",
        community_id="c1",
        member_names=["auth", "session"],
        member_types=["Service", "Service"],
    )
    assert "Community of 2 nodes" in summary


def test_tenant_scoped_community_detection_stub_filters_cross_tenant_records() -> None:
    grouped = tenant_scoped_community_detection_stub(
        records=[
            {"tenant_id": "t1", "community_id": "c1", "node_id": "a"},
            {"tenant_id": "t2", "community_id": "c1", "node_id": "b"},
            {"tenant_id": "t1", "community_id": "c2", "node_id": "c"},
        ],
        tenant_id="t1",
    )
    assert set(grouped) == {"c1", "c2"}
    assert len(grouped["c1"]) == 1
