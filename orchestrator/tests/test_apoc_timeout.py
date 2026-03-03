from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.context_manager import TokenBudget


class TestApocExpansionTimeout:
    @pytest.mark.asyncio
    async def test_apoc_expansion_passes_timeout(self) -> None:
        from orchestrator.app.agentic_traversal import _try_apoc_expansion

        mock_driver = AsyncMock()
        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(return_value={
            "nodes": [], "relationships": [],
        })
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver.session = MagicMock(return_value=mock_session)

        with patch(
            "orchestrator.app.agentic_traversal.apoc_path_expansion",
            new_callable=AsyncMock,
            return_value={"nodes": [], "relationships": []},
        ):
            await _try_apoc_expansion(
                driver=mock_driver,
                start_node_id="svc-1",
                tenant_id="t1",
                acl_params={},
                max_hops=3,
                max_visited=100,
                token_budget=TokenBudget(),
                timeout=5.0,
            )

        mock_session.execute_read.assert_called_once()
        _, kwargs = mock_session.execute_read.call_args
        assert kwargs["timeout"] == 5.0
