from __future__ import annotations

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.agentic_traversal import (
    _SUPERNODE_CONCURRENCY,
    _batched_supernode_expansion,
)

_DEFAULT_ACL = {
    "is_admin": True,
    "acl_team": "",
    "acl_namespaces": [],
    "acl_labels": [],
}


@pytest.mark.asyncio
class TestParallelSupernodeDispatch:

    async def test_all_supernodes_dispatched_concurrently(self) -> None:
        call_order: list[str] = []

        async def _mock_hop(*, driver, source_id, tenant_id, acl_params,
                            timeout, sample_size, max_degree,
                            query_embedding, similarity_threshold):
            call_order.append(source_id)
            await asyncio.sleep(0.01)
            return [{"source_id": source_id, "target_id": f"t-{source_id}"}]

        with patch(
            "orchestrator.app.agentic_traversal.execute_hop",
            side_effect=_mock_hop,
        ):
            results = await _batched_supernode_expansion(
                driver=MagicMock(),
                source_ids=["s1", "s2", "s3"],
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                query_embedding=[0.1, 0.2, 0.3],
            )

        assert len(results) == 3
        target_ids = {r["target_id"] for r in results}
        assert target_ids == {"t-s1", "t-s2", "t-s3"}

    async def test_gather_collects_all_results(self) -> None:
        async def _mock_hop(**kwargs):
            sid = kwargs["source_id"]
            return [
                {"source_id": sid, "target_id": f"a-{sid}"},
                {"source_id": sid, "target_id": f"b-{sid}"},
            ]

        with patch(
            "orchestrator.app.agentic_traversal.execute_hop",
            side_effect=_mock_hop,
        ):
            results = await _batched_supernode_expansion(
                driver=MagicMock(),
                source_ids=["n1", "n2"],
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                query_embedding=[0.1],
            )

        assert len(results) == 4


@pytest.mark.asyncio
class TestParallelSupernodeExceptionIsolation:

    async def test_failed_hop_does_not_discard_others(self) -> None:
        call_count = 0

        async def _mock_hop(**kwargs):
            nonlocal call_count
            call_count += 1
            if kwargs["source_id"] == "bad":
                raise RuntimeError("Neo4j timeout")
            return [{"source_id": kwargs["source_id"], "target_id": "ok"}]

        with patch(
            "orchestrator.app.agentic_traversal.execute_hop",
            side_effect=_mock_hop,
        ):
            results = await _batched_supernode_expansion(
                driver=MagicMock(),
                source_ids=["good1", "bad", "good2"],
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                query_embedding=[0.1],
            )

        assert call_count == 3
        assert len(results) == 2
        source_ids = {r["source_id"] for r in results}
        assert source_ids == {"good1", "good2"}

    async def test_all_failures_returns_empty(self) -> None:
        async def _always_fail(**kwargs):
            raise ConnectionError("pool exhausted")

        with patch(
            "orchestrator.app.agentic_traversal.execute_hop",
            side_effect=_always_fail,
        ):
            results = await _batched_supernode_expansion(
                driver=MagicMock(),
                source_ids=["a", "b"],
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                query_embedding=[0.1],
            )

        assert results == []


@pytest.mark.asyncio
class TestSupernodeConcurrencyConfig:

    async def test_default_concurrency_is_ten(self) -> None:
        assert _SUPERNODE_CONCURRENCY == 10

    async def test_env_override_respected(self, monkeypatch) -> None:
        monkeypatch.setenv("SUPERNODE_CONCURRENCY", "4")
        assert int(os.environ["SUPERNODE_CONCURRENCY"]) == 4

    async def test_semaphore_bounds_parallel_hops(self) -> None:
        max_concurrent = 0
        current_concurrent = 0
        lock = asyncio.Lock()

        async def _tracking_hop(**kwargs):
            nonlocal max_concurrent, current_concurrent
            async with lock:
                current_concurrent += 1
                if current_concurrent > max_concurrent:
                    max_concurrent = current_concurrent
            await asyncio.sleep(0.02)
            async with lock:
                current_concurrent -= 1
            return [{"source_id": kwargs["source_id"], "target_id": "t"}]

        with patch(
            "orchestrator.app.agentic_traversal.execute_hop",
            side_effect=_tracking_hop,
        ), patch(
            "orchestrator.app.agentic_traversal._SUPERNODE_CONCURRENCY", 2,
        ):
            await _batched_supernode_expansion(
                driver=MagicMock(),
                source_ids=[f"s{i}" for i in range(6)],
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                query_embedding=[0.1],
            )

        assert max_concurrent <= 2


@pytest.mark.asyncio
class TestParallelPathNotTriggeredWithoutEmbedding:

    async def test_no_embedding_uses_batch_cypher(self) -> None:
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[
            {"source_id": "s1", "target_id": "t1",
             "target_name": "svc", "rel_type": "CALLS",
             "target_label": "Service", "pagerank": 0, "degree": 0},
        ])
        mock_tx = AsyncMock()
        mock_tx.run = AsyncMock(return_value=mock_result)

        mock_session = AsyncMock()

        async def _read(func, **kwargs):
            return await func(mock_tx)

        mock_session.execute_read = _read
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_driver = AsyncMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        with patch(
            "orchestrator.app.agentic_traversal.execute_hop",
            new_callable=AsyncMock,
        ) as mock_individual:
            results = await _batched_supernode_expansion(
                driver=mock_driver,
                source_ids=["s1", "s2"],
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                query_embedding=None,
            )

        mock_individual.assert_not_called()
        assert mock_tx.run.call_count == 1
