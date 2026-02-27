from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.agentic_traversal import (
    MAX_HOPS,
    bounded_path_expansion,
    run_traversal,
)
from orchestrator.app.context_manager import TokenBudget


_SAMPLE_RESULTS = [
    {"target_id": "svc-a", "target_name": "auth-service", "target_label": "Service"},
    {"target_id": "svc-b", "target_name": "payment-service", "target_label": "Service"},
    {"target_id": "svc-c", "target_name": "user-service", "target_label": "Service"},
]

_DEFAULT_ACL: dict = {"is_admin": True, "acl_team": "", "acl_namespaces": []}


def _mock_bounded_session(return_value=None):
    mock_session = AsyncMock()
    data = return_value if return_value is not None else []
    mock_session.execute_read = AsyncMock(return_value=data)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    return mock_session


def _mock_driver(session):
    driver = MagicMock()
    driver.session.return_value = session
    driver.close = AsyncMock()
    return driver


class TestBoundedPathExpansion:
    @pytest.mark.asyncio
    async def test_bounded_expansion_single_query(self) -> None:
        session = _mock_bounded_session(return_value=_SAMPLE_RESULTS)
        driver = _mock_driver(session)

        results = await bounded_path_expansion(
            driver=driver,
            start_id="root",
            tenant_id="tenant-1",
            acl_params=_DEFAULT_ACL,
            max_hops=3,
        )

        assert len(results) == 3
        assert results[0]["target_id"] == "svc-a"
        assert session.execute_read.call_count == 1

    @pytest.mark.asyncio
    async def test_bounded_expansion_acl_filtering(self) -> None:
        session = _mock_bounded_session(return_value=[])
        driver = _mock_driver(session)

        acl = {"is_admin": False, "acl_team": "platform", "acl_namespaces": ["ns-prod"]}
        await bounded_path_expansion(
            driver=driver,
            start_id="root",
            tenant_id="t-42",
            acl_params=acl,
        )

        session.execute_read.assert_called_once()
        tx_func = session.execute_read.call_args[0][0]

        mock_tx = AsyncMock()
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[])
        mock_tx.run = AsyncMock(return_value=mock_result)
        await tx_func(mock_tx)

        call_kwargs = mock_tx.run.call_args[1]
        assert call_kwargs["is_admin"] is False
        assert call_kwargs["acl_team"] == "platform"
        assert call_kwargs["acl_namespaces"] == ["ns-prod"]

    @pytest.mark.asyncio
    async def test_bounded_expansion_tombstone_filtering(self) -> None:
        session = _mock_bounded_session(return_value=[])
        driver = _mock_driver(session)

        await bounded_path_expansion(
            driver=driver,
            start_id="root",
            tenant_id="t-1",
            acl_params=_DEFAULT_ACL,
        )

        tx_func = session.execute_read.call_args[0][0]

        mock_tx = AsyncMock()
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[])
        mock_tx.run = AsyncMock(return_value=mock_result)
        await tx_func(mock_tx)

        cypher_query = mock_tx.run.call_args[0][0]
        assert "tombstoned_at IS NULL" in cypher_query

    @pytest.mark.asyncio
    async def test_bounded_expansion_max_nodes_limit(self) -> None:
        session = _mock_bounded_session(return_value=_SAMPLE_RESULTS[:1])
        driver = _mock_driver(session)

        await bounded_path_expansion(
            driver=driver,
            start_id="root",
            tenant_id="t-1",
            acl_params=_DEFAULT_ACL,
            max_nodes=25,
        )

        tx_func = session.execute_read.call_args[0][0]

        mock_tx = AsyncMock()
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[])
        mock_tx.run = AsyncMock(return_value=mock_result)
        await tx_func(mock_tx)

        call_kwargs = mock_tx.run.call_args[1]
        assert call_kwargs["max_nodes"] == 25

    @pytest.mark.asyncio
    async def test_bounded_expansion_hop_validation(self) -> None:
        session = _mock_bounded_session()
        driver = _mock_driver(session)

        with pytest.raises(ValueError, match="max_hops"):
            await bounded_path_expansion(
                driver=driver,
                start_id="root",
                tenant_id="t-1",
                acl_params=_DEFAULT_ACL,
                max_hops=MAX_HOPS + 1,
            )

    @pytest.mark.asyncio
    async def test_bounded_expansion_hop_validation_zero(self) -> None:
        session = _mock_bounded_session()
        driver = _mock_driver(session)

        with pytest.raises(ValueError, match="max_hops"):
            await bounded_path_expansion(
                driver=driver,
                start_id="root",
                tenant_id="t-1",
                acl_params=_DEFAULT_ACL,
                max_hops=0,
            )


class TestRunTraversalBounded:
    @pytest.mark.asyncio
    async def test_run_traversal_uses_bounded_first(self) -> None:
        bounded_results = [
            {"target_id": "svc-x", "target_name": "x-service", "target_label": "Service"},
        ]
        with patch(
            "orchestrator.app.agentic_traversal.bounded_path_expansion",
            new_callable=AsyncMock,
            return_value=bounded_results,
        ) as mock_bounded, patch(
            "orchestrator.app.agentic_traversal.execute_hop",
            new_callable=AsyncMock,
        ) as mock_hop:
            driver = MagicMock()

            context = await run_traversal(
                driver=driver,
                start_node_id="root",
                tenant_id="t-1",
                acl_params=_DEFAULT_ACL,
                max_hops=3,
            )

            mock_bounded.assert_called_once()
            mock_hop.assert_not_called()
            assert any(r["target_id"] == "svc-x" for r in context)

    @pytest.mark.asyncio
    async def test_run_traversal_falls_back_on_timeout(self) -> None:
        hop_results = [
            {"target_id": "svc-fallback", "target_name": "fb", "target_label": "Service"},
        ]
        with patch(
            "orchestrator.app.agentic_traversal.bounded_path_expansion",
            new_callable=AsyncMock,
            side_effect=RuntimeError("query timeout"),
        ), patch(
            "orchestrator.app.agentic_traversal.execute_hop",
            new_callable=AsyncMock,
            return_value=hop_results,
        ) as mock_hop:
            driver = MagicMock()

            context = await run_traversal(
                driver=driver,
                start_node_id="root",
                tenant_id="t-1",
                acl_params=_DEFAULT_ACL,
                max_hops=2,
            )

            mock_hop.assert_called()
            assert any(r.get("target_id") == "svc-fallback" for r in context)

    @pytest.mark.asyncio
    async def test_run_traversal_token_budget_applied(self) -> None:
        many_results = [
            {
                "target_id": f"svc-{i}",
                "target_name": f"service-{'x' * 200}",
                "target_label": "Service",
            }
            for i in range(50)
        ]
        with patch(
            "orchestrator.app.agentic_traversal.bounded_path_expansion",
            new_callable=AsyncMock,
            return_value=many_results,
        ):
            driver = MagicMock()

            context = await run_traversal(
                driver=driver,
                start_node_id="root",
                tenant_id="t-1",
                acl_params=_DEFAULT_ACL,
                max_hops=3,
                token_budget=TokenBudget(max_context_tokens=100, max_results=5),
            )

            assert len(context) <= 5
            assert len(context) < len(many_results)
