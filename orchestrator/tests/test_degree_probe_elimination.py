from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from neo4j.exceptions import ClientError

from orchestrator.app.agentic_traversal import (
    TraversalConfig,
    TraversalStrategy,
    run_traversal,
)


class TestAdaptiveDegreeHint:
    @pytest.mark.asyncio
    async def test_high_degree_hint_routes_to_batched_bfs(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.ADAPTIVE,
            degree_threshold=200,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()
        expected = [{"target_id": "svc-dense"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal._try_apoc_expansion",
                new_callable=AsyncMock,
                side_effect=ClientError("not available"),
            ),
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=expected,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-gateway",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
                degree_hint=500,
            )
            mock_bfs.assert_called_once()
            assert result == expected

    @pytest.mark.asyncio
    async def test_low_degree_hint_routes_to_bounded_cypher(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.ADAPTIVE,
            degree_threshold=200,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()
        expected = [{"target_id": "svc-leaf"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal._try_apoc_expansion",
                new_callable=AsyncMock,
                side_effect=ClientError("not available"),
            ),
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=expected,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-leaf",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
                degree_hint=50,
            )
            mock_bfs.assert_called_once()
            assert result == expected

    @pytest.mark.asyncio
    async def test_no_degree_hint_defaults_to_bounded_cypher(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.ADAPTIVE,
            degree_threshold=200,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()
        expected = [{"target_id": "svc-safe"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal._try_apoc_expansion",
                new_callable=AsyncMock,
                side_effect=ClientError("not available"),
            ),
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=expected,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-unknown",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
            )
            mock_bfs.assert_called_once()
            assert result == expected

    @pytest.mark.asyncio
    async def test_degree_hint_at_threshold_routes_to_batched(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.ADAPTIVE,
            degree_threshold=200,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()
        expected = [{"target_id": "svc-boundary"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal._try_apoc_expansion",
                new_callable=AsyncMock,
                side_effect=ClientError("not available"),
            ),
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=expected,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-boundary",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
                degree_hint=200,
            )
            mock_bfs.assert_called_once()
            assert result == expected

    @pytest.mark.asyncio
    async def test_degree_hint_zero_routes_to_bounded(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.ADAPTIVE,
            degree_threshold=200,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()
        expected = [{"target_id": "svc-zero"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal._try_apoc_expansion",
                new_callable=AsyncMock,
                side_effect=ClientError("not available"),
            ),
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=expected,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-zero",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
                degree_hint=0,
            )
            mock_bfs.assert_called_once()
            assert result == expected


class TestNonAdaptiveIgnoresDegreeHint:
    @pytest.mark.asyncio
    async def test_bounded_strategy_ignores_degree_hint(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.BOUNDED_CYPHER,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()
        expected = [{"target_id": "svc-bounded"}]

        with patch(
            "orchestrator.app.agentic_traversal.bounded_path_expansion",
            new_callable=AsyncMock,
            return_value=expected,
        ):
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
                degree_hint=9999,
            )
            assert result == expected

    @pytest.mark.asyncio
    async def test_batched_strategy_ignores_degree_hint(self) -> None:
        config = TraversalConfig(
            strategy=TraversalStrategy.BATCHED_BFS,
        )
        acl_params = {"is_admin": True, "acl_team": "", "acl_namespaces": []}
        mock_driver = MagicMock()
        expected = [{"target_id": "svc-batched"}]

        with patch(
            "orchestrator.app.agentic_traversal._batched_bfs",
            new_callable=AsyncMock,
            return_value=expected,
        ):
            result = await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=acl_params,
                config=config,
                degree_hint=0,
            )
            assert result == expected


class TestProbeStartDegreeRemoved:
    def test_probe_start_degree_not_importable(self) -> None:
        from orchestrator.app import agentic_traversal

        assert not hasattr(agentic_traversal, "_probe_start_degree")


class TestExtractStartNodeDegree:
    def test_extracts_degree_from_candidate_metadata(self) -> None:
        from orchestrator.app.query_engine import extract_start_node_degree

        candidates = [
            {"id": "svc-a", "name": "auth-service", "degree": 350, "score": 0.95},
            {"id": "svc-b", "name": "db-service", "degree": 10, "score": 0.80},
        ]
        assert extract_start_node_degree(candidates) == 350

    def test_returns_none_when_no_degree_in_metadata(self) -> None:
        from orchestrator.app.query_engine import extract_start_node_degree

        candidates = [
            {"id": "svc-a", "name": "auth-service", "score": 0.95},
        ]
        assert extract_start_node_degree(candidates) is None

    def test_returns_none_for_empty_candidates(self) -> None:
        from orchestrator.app.query_engine import extract_start_node_degree

        assert extract_start_node_degree([]) is None

    def test_returns_integer_degree(self) -> None:
        from orchestrator.app.query_engine import extract_start_node_degree

        candidates = [{"id": "svc-a", "degree": 42.0}]
        result = extract_start_node_degree(candidates)
        assert result == 42
        assert isinstance(result, int)
