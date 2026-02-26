from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.query_models import QueryComplexity
from orchestrator.tests.conftest import mock_neo4j_driver_with_session


def _make_state(base_query_state, **overrides):
    return {**base_query_state, **overrides}


def _make_neo4j_session(run_return=None, run_side_effect=None):
    mock_session = AsyncMock()
    if run_side_effect:
        mock_session.execute_read = AsyncMock(side_effect=run_side_effect)
    else:
        mock_session.execute_read = AsyncMock(return_value=run_return or [])
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    return mock_session


class TestCypherRetrieveUsesAgenticTraversal:
    @pytest.mark.asyncio
    async def test_cypher_retrieve_uses_traversal_when_no_template(
        self, base_query_state,
    ) -> None:
        from orchestrator.app.query_engine import cypher_retrieve

        traversal_context = [
            {"target_id": "svc-b", "target_name": "payment", "rel_type": "CALLS"},
        ]
        mock_session = _make_neo4j_session()
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="What is the blast radius if auth fails?",
            retrieval_path="cypher",
            tenant_id="t1",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine.resolve_driver_for_tenant",
            return_value=(mock_driver, "tenant-db"),
        ), patch(
            "orchestrator.app.query_engine._try_template_match",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "orchestrator.app.query_engine._fetch_candidates",
            new_callable=AsyncMock,
            return_value=[{"name": "auth-service", "id": "auth-svc-1"}],
        ), patch(
            "orchestrator.app.query_engine.run_traversal",
            new_callable=AsyncMock,
            return_value=traversal_context,
        ) as mock_traversal:
            result = await cypher_retrieve(state)

        mock_traversal.assert_called_once()
        assert result["cypher_results"] == traversal_context

    @pytest.mark.asyncio
    async def test_cypher_retrieve_still_uses_template_when_matched(
        self, base_query_state,
    ) -> None:
        from orchestrator.app.query_engine import cypher_retrieve

        template_result = {
            "cypher_query": "MATCH ...",
            "cypher_results": [{"name": "result"}],
            "iteration_count": 0,
        }
        mock_session = _make_neo4j_session()
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="What is the blast radius of auth-service?",
            retrieval_path="cypher",
            tenant_id="t1",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine.resolve_driver_for_tenant",
            return_value=(mock_driver, "tenant-db"),
        ), patch(
            "orchestrator.app.query_engine._try_template_match",
            new_callable=AsyncMock,
            return_value=template_result,
        ):
            result = await cypher_retrieve(state)

        assert result == template_result

    @pytest.mark.asyncio
    async def test_cypher_retrieve_returns_empty_when_no_candidates(
        self, base_query_state,
    ) -> None:
        from orchestrator.app.query_engine import cypher_retrieve

        mock_session = _make_neo4j_session()
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="What is the full dependency chain?",
            retrieval_path="cypher",
            tenant_id="t1",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine.resolve_driver_for_tenant",
            return_value=(mock_driver, "tenant-db"),
        ), patch(
            "orchestrator.app.query_engine._try_template_match",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "orchestrator.app.query_engine._fetch_candidates",
            new_callable=AsyncMock,
            return_value=[],
        ):
            result = await cypher_retrieve(state)

        assert result["cypher_results"] == []


class TestDynamicCypherRemoved:
    def test_no_generate_cypher_in_query_engine(self) -> None:
        import orchestrator.app.query_engine as qe

        assert not hasattr(qe, "_generate_cypher"), (
            "_generate_cypher should be removed from query_engine"
        )

    def test_no_dynamic_cypher_allowed_import(self) -> None:
        import orchestrator.app.query_engine as qe

        assert not hasattr(qe, "dynamic_cypher_allowed"), (
            "dynamic_cypher_allowed should not be imported in query_engine"
        )


class TestHybridRetrieveNoLLMCypher:
    @pytest.mark.asyncio
    async def test_hybrid_uses_template_not_llm(self, base_query_state) -> None:
        from orchestrator.app.query_engine import hybrid_retrieve

        mock_session = _make_neo4j_session(
            run_side_effect=[
                [{"name": "svc-a", "score": 0.9}],
                [{"name": "svc-a", "dep_count": 5}],
            ]
        )
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="What are the most critical services by dependency count?",
            retrieval_path="hybrid",
            tenant_id="t1",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine.resolve_driver_for_tenant",
            return_value=(mock_driver, "tenant-db"),
        ):
            result = await hybrid_retrieve(state)

        assert "candidates" in result
