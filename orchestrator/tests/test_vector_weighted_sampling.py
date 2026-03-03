from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from orchestrator.app.agentic_traversal import (
    _batched_supernode_expansion,
    execute_batched_hop,
)


class TestBatchedHopAcceptsQueryEmbedding:

    @pytest.mark.asyncio
    async def test_execute_batched_hop_signature_includes_query_embedding(
        self,
    ) -> None:
        import inspect
        sig = inspect.signature(execute_batched_hop)
        assert "query_embedding" in sig.parameters

    @pytest.mark.asyncio
    async def test_batched_supernode_expansion_accepts_query_embedding(
        self,
    ) -> None:
        import inspect
        sig = inspect.signature(_batched_supernode_expansion)
        assert "query_embedding" in sig.parameters


class TestSupernodeSemanticSampling:

    @pytest.mark.asyncio
    @patch("orchestrator.app.agentic_traversal.execute_hop")
    async def test_supernode_expansion_delegates_to_execute_hop_with_embedding(
        self, mock_execute_hop: AsyncMock,
    ) -> None:
        mock_execute_hop.return_value = [
            {
                "source_id": "svc-big",
                "target_id": "svc-child",
                "target_name": "svc-child",
                "rel_type": "CALLS",
                "target_label": "Service",
            },
        ]
        mock_driver = MagicMock()
        query_embedding = [0.1] * 768

        results = await _batched_supernode_expansion(
            driver=mock_driver,
            source_ids=["svc-big"],
            tenant_id="t1",
            acl_params={"is_admin": True, "acl_labels": []},
            timeout=5.0,
            sample_size=50,
            query_embedding=query_embedding,
        )

        mock_execute_hop.assert_called_once()
        _, kwargs = mock_execute_hop.call_args
        assert kwargs["query_embedding"] is query_embedding
        assert kwargs["source_id"] == "svc-big"
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_supernode_expansion_falls_back_without_embedding(
        self,
    ) -> None:
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[
            {"source_id": "svc-big", "target_id": "svc-child"},
        ])
        mock_tx = AsyncMock()
        mock_tx.run = AsyncMock(return_value=mock_result)
        mock_session = AsyncMock()

        async def _exec_read(fn, **kw):
            return await fn(mock_tx)

        mock_session.execute_read = AsyncMock(side_effect=_exec_read)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = MagicMock()
        mock_driver.session.return_value = mock_session

        await _batched_supernode_expansion(
            driver=mock_driver,
            source_ids=["svc-big"],
            tenant_id="t1",
            acl_params={"is_admin": True, "acl_labels": []},
            timeout=5.0,
            sample_size=50,
            query_embedding=None,
        )

        call_args = mock_tx.run.call_args
        params_used = call_args.kwargs
        assert "query_embedding" not in params_used

    @pytest.mark.asyncio
    @patch(
        "orchestrator.app.agentic_traversal._batched_supernode_expansion",
    )
    @patch("orchestrator.app.agentic_traversal.batch_check_degrees")
    async def test_execute_batched_hop_threads_embedding_to_supernodes(
        self,
        mock_batch_degrees: AsyncMock,
        mock_supernode_expansion: AsyncMock,
    ) -> None:
        mock_batch_degrees.return_value = {"supernode-1": 1000}
        mock_supernode_expansion.return_value = [
            {"source_id": "supernode-1", "target_id": "child-1"},
        ]
        mock_driver = MagicMock()
        query_embedding = [0.5] * 768

        await execute_batched_hop(
            driver=mock_driver,
            source_ids=["supernode-1"],
            tenant_id="t1",
            acl_params={"is_admin": True, "acl_labels": []},
            timeout=5.0,
            degree_threshold=500,
            query_embedding=query_embedding,
        )

        mock_supernode_expansion.assert_called_once()
        _, kwargs = mock_supernode_expansion.call_args
        assert kwargs["query_embedding"] is query_embedding

    @pytest.mark.asyncio
    @patch("orchestrator.app.agentic_traversal.execute_hop")
    async def test_multiple_supernodes_each_get_embedding(
        self, mock_execute_hop: AsyncMock,
    ) -> None:
        mock_execute_hop.return_value = [
            {"source_id": "svc", "target_id": "child"},
        ]
        mock_driver = MagicMock()
        query_embedding = [0.2] * 768

        await _batched_supernode_expansion(
            driver=mock_driver,
            source_ids=["svc-a", "svc-b", "svc-c"],
            tenant_id="t1",
            acl_params={"is_admin": True, "acl_labels": []},
            timeout=5.0,
            sample_size=50,
            query_embedding=query_embedding,
        )

        assert mock_execute_hop.call_count == 3
        for c in mock_execute_hop.call_args_list:
            assert c.kwargs["query_embedding"] is query_embedding
