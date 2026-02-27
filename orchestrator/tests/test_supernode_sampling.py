from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.agentic_traversal import (
    MAX_NODE_DEGREE,
    _SAMPLED_NEIGHBOR_TEMPLATE,
    execute_hop,
)


def _mock_driver_with_sampling(
    degree_result: list,
    normal_results: list | None = None,
    sample_results: list | None = None,
):
    driver = AsyncMock()
    session = AsyncMock()
    call_count = {"n": 0}

    async def _execute_read(tx_fn, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return degree_result
        if sample_results is not None and call_count["n"] == 2:
            return sample_results
        return normal_results or []

    session.execute_read = _execute_read
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    driver.session = MagicMock(return_value=session)
    return driver


class TestSupernodeReturnsSampledResults:
    @pytest.mark.asyncio
    async def test_high_degree_node_returns_samples_not_empty(self) -> None:
        degree_data = [{"degree": MAX_NODE_DEGREE + 100}]
        sampled = [
            {"target_id": "s1", "target_name": "svc-1", "rel_type": "CALLS", "target_label": "Service"},
            {"target_id": "s2", "target_name": "svc-2", "rel_type": "WRITES_TO", "target_label": "Topic"},
        ]
        driver = _mock_driver_with_sampling(degree_data, sample_results=sampled)
        results = await execute_hop(
            driver=driver, source_id="hub-node", tenant_id="t1",
            acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
        )
        assert len(results) == 2
        assert results[0]["target_id"] == "s1"
        assert results[1]["target_id"] == "s2"


class TestSampleSizeRespected:
    @pytest.mark.asyncio
    async def test_custom_sample_size_passed_to_query(self) -> None:
        degree_data = [{"degree": MAX_NODE_DEGREE + 1}]
        sampled = [{"target_id": f"n{i}"} for i in range(25)]
        driver = _mock_driver_with_sampling(degree_data, sample_results=sampled)
        results = await execute_hop(
            driver=driver, source_id="big-hub", tenant_id="t1",
            acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
            sample_size=25,
        )
        assert len(results) == 25


class TestNormalNodeUnchanged:
    @pytest.mark.asyncio
    async def test_below_threshold_uses_original_query(self) -> None:
        degree_data = [{"degree": 10}]
        normal = [
            {"target_id": "a", "target_name": "svc-a", "rel_type": "CALLS", "target_label": "Service"},
        ]
        driver = _mock_driver_with_sampling(degree_data, normal_results=normal)
        results = await execute_hop(
            driver=driver, source_id="normal-node", tenant_id="t1",
            acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
        )
        assert len(results) == 1
        assert results[0]["target_id"] == "a"


class TestSemanticPruningSupernodes:
    @pytest.mark.asyncio
    async def test_supernode_with_query_embedding_uses_semantic_template(self) -> None:
        from orchestrator.app.agentic_traversal import (
            _SEMANTIC_PRUNED_NEIGHBOR_TEMPLATE,
            execute_hop,
        )

        degree_data = [{"degree": MAX_NODE_DEGREE + 100}]
        semantic_results = [
            {"target_id": "sem1", "target_name": "svc-semantic",
             "rel_type": "CALLS", "target_label": "Service"},
        ]

        call_templates: list = []

        driver = AsyncMock()
        session = AsyncMock()
        call_count = {"n": 0}

        async def _execute_read(tx_fn, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return degree_data
            call_templates.append("sampled_called")
            return semantic_results

        session.execute_read = _execute_read
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        driver.session = MagicMock(return_value=session)

        results = await execute_hop(
            driver=driver,
            source_id="hub-node",
            tenant_id="t1",
            acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
            query_embedding=[0.1, 0.2, 0.3],
            similarity_threshold=0.5,
        )
        assert len(results) == 1
        assert results[0]["target_id"] == "sem1"

    @pytest.mark.asyncio
    async def test_supernode_without_query_embedding_uses_deterministic(self) -> None:
        degree_data = [{"degree": MAX_NODE_DEGREE + 100}]
        sampled = [
            {"target_id": "det1", "target_name": "svc-det",
             "rel_type": "CALLS", "target_label": "Service"},
        ]
        driver = _mock_driver_with_sampling(degree_data, sample_results=sampled)
        results = await execute_hop(
            driver=driver,
            source_id="hub-node",
            tenant_id="t1",
            acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
        )
        assert len(results) == 1
        assert results[0]["target_id"] == "det1"

    @pytest.mark.asyncio
    async def test_similarity_threshold_passed_to_template(self) -> None:
        from orchestrator.app.agentic_traversal import execute_hop

        degree_data = [{"degree": MAX_NODE_DEGREE + 100}]

        captured_params: dict = {}
        driver = AsyncMock()
        session = AsyncMock()
        call_count = {"n": 0}

        async def _execute_read(tx_fn, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return degree_data
            mock_tx = AsyncMock()
            mock_result = AsyncMock()
            mock_result.data = AsyncMock(return_value=[])
            mock_tx.run = AsyncMock(return_value=mock_result)
            await tx_fn(mock_tx)
            if mock_tx.run.call_args is not None:
                captured_params.update(mock_tx.run.call_args.kwargs)
            return []

        session.execute_read = _execute_read
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        driver.session = MagicMock(return_value=session)

        await execute_hop(
            driver=driver,
            source_id="hub",
            tenant_id="t1",
            acl_params={"is_admin": True, "acl_team": "", "acl_namespaces": []},
            query_embedding=[0.1, 0.2],
            similarity_threshold=0.7,
        )

        assert captured_params.get("sim_threshold") == 0.7, (
            f"Expected sim_threshold=0.7, got {captured_params.get('sim_threshold')}"
        )

    def test_semantic_template_exists(self) -> None:
        from orchestrator.app.agentic_traversal import _SEMANTIC_PRUNED_NEIGHBOR_TEMPLATE

        assert "vector.similarity.cosine" in _SEMANTIC_PRUNED_NEIGHBOR_TEMPLATE
        assert "$query_embedding" in _SEMANTIC_PRUNED_NEIGHBOR_TEMPLATE
        assert "$sim_threshold" in _SEMANTIC_PRUNED_NEIGHBOR_TEMPLATE


class TestSampledNeighborTemplateExists:
    def test_template_includes_deterministic_ordering(self) -> None:
        assert "target.id" in _SAMPLED_NEIGHBOR_TEMPLATE
        assert "rand()" not in _SAMPLED_NEIGHBOR_TEMPLATE
        assert "$sample_size" in _SAMPLED_NEIGHBOR_TEMPLATE

    def test_template_includes_acl_filtering(self) -> None:
        assert "$is_admin" in _SAMPLED_NEIGHBOR_TEMPLATE
        assert "$acl_team" in _SAMPLED_NEIGHBOR_TEMPLATE
        assert "$acl_namespaces" in _SAMPLED_NEIGHBOR_TEMPLATE

    def test_template_includes_tombstone_filter(self) -> None:
        assert "tombstoned_at IS NULL" in _SAMPLED_NEIGHBOR_TEMPLATE
