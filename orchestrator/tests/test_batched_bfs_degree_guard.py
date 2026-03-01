from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.agentic_traversal import (
    MAX_NODE_DEGREE,
    execute_batched_hop,
)


_DEFAULT_ACL = {
    "is_admin": True,
    "acl_team": "",
    "acl_namespaces": [],
}


def _make_degree_map(node_degrees: dict[str, int]) -> dict[str, int]:
    return node_degrees


class TestBatchedHopDegreeGuard:

    @pytest.mark.asyncio
    async def test_super_nodes_excluded_from_batch_query(self) -> None:
        frontier = ["normal-1", "normal-2", "super-hub"]
        degree_map = {
            "normal-1": 10,
            "normal-2": 5,
            "super-hub": MAX_NODE_DEGREE + 100,
        }

        batch_query_source_ids: list[list[str]] = []

        async def _mock_batch_check(driver, source_ids, tenant_id, timeout=30.0):
            return {sid: degree_map.get(sid, 0) for sid in source_ids}

        with patch(
            "orchestrator.app.agentic_traversal.batch_check_degrees",
            side_effect=_mock_batch_check,
        ), patch(
            "orchestrator.app.agentic_traversal.execute_hop",
            new_callable=AsyncMock,
            return_value=[
                {"source_id": "super-hub", "target_id": "sampled-1",
                 "target_name": "s1", "rel_type": "CALLS",
                 "target_label": "Service", "pagerank": 0, "degree": 0},
            ],
        ) as mock_hop:
            mock_session = AsyncMock()

            async def _capture_batch_read(func, **kwargs):
                mock_tx = AsyncMock()

                async def _run(query, **params):
                    if "frontier_ids" in params:
                        batch_query_source_ids.append(params["frontier_ids"])
                    result = AsyncMock()
                    result.data = AsyncMock(return_value=[
                        {"source_id": "normal-1", "target_id": "n1-target",
                         "target_name": "t1", "rel_type": "CALLS",
                         "target_label": "Service", "pagerank": 0, "degree": 0},
                    ])
                    return result

                mock_tx.run = _run
                return await func(mock_tx)

            mock_session.execute_read = _capture_batch_read
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_driver = AsyncMock()
            mock_driver.session = MagicMock(return_value=mock_session)

            results = await execute_batched_hop(
                driver=mock_driver,
                source_ids=frontier,
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
            )

            assert len(batch_query_source_ids) >= 1, (
                "Batch query must be executed for normal-degree nodes"
            )
            for batch_ids in batch_query_source_ids:
                assert "super-hub" not in batch_ids, (
                    "Super-node must NOT appear in the batch UNWIND query. "
                    "It should be sampled individually via execute_hop."
                )

            mock_hop.assert_called_once()
            hop_kwargs = mock_hop.call_args
            assert hop_kwargs[1].get("source_id") == "super-hub" or \
                hop_kwargs[0][1] == "super-hub", (
                "Super-node must be sampled via execute_hop"
            )

    @pytest.mark.asyncio
    async def test_all_normal_nodes_go_through_batch(self) -> None:
        frontier = ["a", "b", "c"]
        degree_map = {"a": 5, "b": 10, "c": 20}

        batch_query_source_ids: list[list[str]] = []

        async def _mock_batch_check(driver, source_ids, tenant_id, timeout=30.0):
            return {sid: degree_map.get(sid, 0) for sid in source_ids}

        with patch(
            "orchestrator.app.agentic_traversal.batch_check_degrees",
            side_effect=_mock_batch_check,
        ), patch(
            "orchestrator.app.agentic_traversal.execute_hop",
            new_callable=AsyncMock,
        ) as mock_hop:
            mock_session = AsyncMock()

            async def _capture_batch_read(func, **kwargs):
                mock_tx = AsyncMock()

                async def _run(query, **params):
                    if "frontier_ids" in params:
                        batch_query_source_ids.append(params["frontier_ids"])
                    result = AsyncMock()
                    result.data = AsyncMock(return_value=[])
                    return result

                mock_tx.run = _run
                return await func(mock_tx)

            mock_session.execute_read = _capture_batch_read
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_driver = AsyncMock()
            mock_driver.session = MagicMock(return_value=mock_session)

            await execute_batched_hop(
                driver=mock_driver,
                source_ids=frontier,
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
            )

            mock_hop.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_frontier_skips_degree_check(self) -> None:
        with patch(
            "orchestrator.app.agentic_traversal.batch_check_degrees",
            new_callable=AsyncMock,
        ) as mock_degrees:
            mock_driver = AsyncMock()

            results = await execute_batched_hop(
                driver=mock_driver,
                source_ids=[],
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
            )

            assert results == []
            mock_degrees.assert_not_called()

    @pytest.mark.asyncio
    async def test_super_node_results_merged_with_batch(self) -> None:
        frontier = ["normal", "hub"]
        degree_map = {"normal": 10, "hub": MAX_NODE_DEGREE + 1}

        async def _mock_batch_check(driver, source_ids, tenant_id, timeout=30.0):
            return {sid: degree_map.get(sid, 0) for sid in source_ids}

        batch_result = {"source_id": "normal", "target_id": "from-batch",
                        "target_name": "b", "rel_type": "CALLS",
                        "target_label": "Service", "pagerank": 0, "degree": 0}
        sampled_result = {"source_id": "hub", "target_id": "from-sample",
                          "target_name": "s", "rel_type": "CALLS",
                          "target_label": "Service", "pagerank": 0, "degree": 0}

        with patch(
            "orchestrator.app.agentic_traversal.batch_check_degrees",
            side_effect=_mock_batch_check,
        ), patch(
            "orchestrator.app.agentic_traversal.execute_hop",
            new_callable=AsyncMock,
            return_value=[sampled_result],
        ):
            mock_session = AsyncMock()

            async def _batch_read(func, **kwargs):
                mock_tx = AsyncMock()

                async def _run(query, **params):
                    result = AsyncMock()
                    result.data = AsyncMock(return_value=[batch_result])
                    return result

                mock_tx.run = _run
                return await func(mock_tx)

            mock_session.execute_read = _batch_read
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_driver = AsyncMock()
            mock_driver.session = MagicMock(return_value=mock_session)

            results = await execute_batched_hop(
                driver=mock_driver,
                source_ids=frontier,
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
            )

            target_ids = {r["target_id"] for r in results}
            assert "from-batch" in target_ids, "Batch results must be included"
            assert "from-sample" in target_ids, "Sampled super-node results must be included"
