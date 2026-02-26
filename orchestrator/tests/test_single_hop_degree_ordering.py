from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.query_models import QueryComplexity


class TestSingleHopDegreeOrdering:

    @pytest.mark.asyncio
    async def test_hop_cypher_includes_order_by_degree_desc(self) -> None:
        captured_queries: List[str] = []

        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[
            {"source": "auth", "rel": "CALLS", "target": "db"},
        ])

        mock_tx = AsyncMock()
        async def _capture_run(query, **kwargs):
            captured_queries.append(query)
            return mock_result
        mock_tx.run = _capture_run

        async def _mock_execute_read(fn, **kw):
            return await fn(mock_tx)

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(side_effect=_mock_execute_read)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        vector_results = [{"name": "auth-service", "score": 0.9}]

        state: Dict[str, Any] = {
            "query": "What does auth-service depend on?",
            "max_results": 5,
            "complexity": QueryComplexity.SINGLE_HOP,
            "retrieval_path": "single_hop",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
            "authorization": "",
            "tenant_id": "",
        }

        with (
            patch(
                "orchestrator.app.query_engine._get_neo4j_driver",
                return_value=mock_driver,
            ),
            patch(
                "orchestrator.app.query_engine._fetch_candidates",
                new_callable=AsyncMock,
                return_value=vector_results,
            ),
            patch(
                "orchestrator.app.query_engine._apply_acl",
                side_effect=lambda cypher, st, alias="n": (cypher, {}),
            ),
        ):
            from orchestrator.app.query_engine import single_hop_retrieve
            await single_hop_retrieve(state)

        hop_queries = [q for q in captured_queries if "MATCH" in q]
        assert len(hop_queries) >= 1, "Expected at least one hop query"

        hop_query = hop_queries[0]
        assert "ORDER BY" in hop_query, (
            f"Hop query must contain ORDER BY for degree-aware ordering: {hop_query}"
        )
        assert "size((n)--())" in hop_query, (
            f"Hop query must order by node degree size((n)--()):  {hop_query}"
        )
        assert hop_query.index("ORDER BY") < hop_query.index("LIMIT"), (
            "ORDER BY must appear before LIMIT"
        )
