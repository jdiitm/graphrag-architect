from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.community_summary import (
    CommunitySummary,
    CommunitySummaryStore,
    retrieve_community_summaries,
)
from orchestrator.app.semantic_partitioner import GDSPartitioner, PartitionResult


class TestCommunitySummaryModel:

    def test_community_summary_fields(self) -> None:
        summary = CommunitySummary(
            community_id="community-0",
            tenant_id="t1",
            member_count=5,
            summary_text="This community handles auth services.",
            member_ids=frozenset({"svc-a", "svc-b", "svc-c", "svc-d", "svc-e"}),
        )
        assert summary.community_id == "community-0"
        assert summary.tenant_id == "t1"
        assert summary.member_count == 5
        assert "auth" in summary.summary_text
        assert len(summary.member_ids) == 5


class TestCommunitySummaryStore:

    def test_store_and_retrieve(self) -> None:
        store = CommunitySummaryStore()
        summary = CommunitySummary(
            community_id="community-0",
            tenant_id="t1",
            member_count=3,
            summary_text="Auth services cluster.",
            member_ids=frozenset({"a", "b", "c"}),
        )
        store.upsert(summary)
        results = store.get_by_tenant("t1")
        assert len(results) == 1
        assert results[0].community_id == "community-0"

    def test_retrieve_empty_tenant(self) -> None:
        store = CommunitySummaryStore()
        results = store.get_by_tenant("nonexistent")
        assert results == []

    def test_upsert_replaces_existing(self) -> None:
        store = CommunitySummaryStore()
        s1 = CommunitySummary(
            community_id="c0",
            tenant_id="t1",
            member_count=2,
            summary_text="Old summary.",
            member_ids=frozenset({"a", "b"}),
        )
        store.upsert(s1)
        s2 = CommunitySummary(
            community_id="c0",
            tenant_id="t1",
            member_count=3,
            summary_text="Updated summary.",
            member_ids=frozenset({"a", "b", "c"}),
        )
        store.upsert(s2)
        results = store.get_by_tenant("t1")
        assert len(results) == 1
        assert results[0].summary_text == "Updated summary."


class TestRetrieveCommunitySummaries:

    def test_returns_summaries_for_tenant(self) -> None:
        store = CommunitySummaryStore()
        store.upsert(CommunitySummary(
            community_id="c0",
            tenant_id="t1",
            member_count=2,
            summary_text="Summary A.",
            member_ids=frozenset({"a", "b"}),
        ))
        store.upsert(CommunitySummary(
            community_id="c1",
            tenant_id="t1",
            member_count=3,
            summary_text="Summary B.",
            member_ids=frozenset({"c", "d", "e"}),
        ))
        results = retrieve_community_summaries(store, "t1")
        assert len(results) == 2

    def test_isolates_tenants(self) -> None:
        store = CommunitySummaryStore()
        store.upsert(CommunitySummary(
            community_id="c0",
            tenant_id="t1",
            member_count=1,
            summary_text="T1 only.",
            member_ids=frozenset({"x"}),
        ))
        results = retrieve_community_summaries(store, "t2")
        assert results == []


class TestGDSPartitionerLeiden:

    @pytest.mark.asyncio
    async def test_partition_async_uses_leiden(self) -> None:
        mock_driver = MagicMock()
        mock_session = AsyncMock()
        mock_result = AsyncMock()
        mock_result.data.return_value = [
            {"nodeId": "svc-a", "communityId": 0},
            {"nodeId": "svc-b", "communityId": 0},
            {"nodeId": "svc-c", "communityId": 1},
        ]
        mock_session.execute_read = AsyncMock(return_value=mock_result.data.return_value)
        mock_driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_driver.session.return_value.__aexit__ = AsyncMock(return_value=False)

        partitioner = GDSPartitioner(driver=mock_driver)
        result = await partitioner.partition_async(
            graph_name="test-graph",
            node_label="Service",
        )

        call_args = mock_session.execute_read.call_args
        tx_fn = call_args[0][0]
        mock_tx = AsyncMock()
        mock_tx_result = AsyncMock()
        mock_tx_result.data.return_value = []
        mock_tx.run.return_value = mock_tx_result
        await tx_fn(mock_tx)

        cypher_query = mock_tx.run.call_args[0][0]
        assert "leiden" in cypher_query.lower()
