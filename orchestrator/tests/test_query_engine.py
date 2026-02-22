from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.query_models import QueryComplexity
from orchestrator.tests.conftest import mock_neo4j_driver_with_session


def _make_state(base_query_state, **overrides):
    return {**base_query_state, **overrides}


def _make_neo4j_session(run_return=None, run_side_effect=None):
    mock_session = AsyncMock()
    if run_side_effect:
        mock_session.run = AsyncMock(side_effect=run_side_effect)
    else:
        mock_result = MagicMock()
        mock_result.data.return_value = run_return or []
        mock_session.run = AsyncMock(return_value=mock_result)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    return mock_session


class TestClassifyNode:
    def test_sets_entity_lookup(self, base_query_state):
        from orchestrator.app.query_engine import classify_query_node

        state = _make_state(base_query_state, query="What language is auth-service?")
        result = classify_query_node(state)
        assert result["complexity"] == QueryComplexity.ENTITY_LOOKUP
        assert result["retrieval_path"] == "vector"

    def test_sets_multi_hop(self, base_query_state):
        from orchestrator.app.query_engine import classify_query_node

        state = _make_state(base_query_state, query="What is the blast radius if auth fails?")
        result = classify_query_node(state)
        assert result["complexity"] == QueryComplexity.MULTI_HOP
        assert result["retrieval_path"] == "cypher"


class TestRouteQuery:
    def test_routes_entity_lookup_to_vector(self, base_query_state):
        from orchestrator.app.query_engine import route_query

        state = _make_state(base_query_state, retrieval_path="vector")
        assert route_query(state) == "vector_retrieve"

    def test_routes_single_hop_to_single_hop(self, base_query_state):
        from orchestrator.app.query_engine import route_query

        state = _make_state(base_query_state, retrieval_path="single_hop")
        assert route_query(state) == "single_hop_retrieve"

    def test_routes_multi_hop_to_cypher(self, base_query_state):
        from orchestrator.app.query_engine import route_query

        state = _make_state(base_query_state, retrieval_path="cypher")
        assert route_query(state) == "cypher_retrieve"

    def test_routes_aggregate_to_hybrid(self, base_query_state):
        from orchestrator.app.query_engine import route_query

        state = _make_state(base_query_state, retrieval_path="hybrid")
        assert route_query(state) == "hybrid_retrieve"


class TestVectorRetrieve:
    @pytest.mark.asyncio
    async def test_returns_candidates_from_neo4j(self, base_query_state):
        from orchestrator.app.query_engine import vector_retrieve

        mock_records = [
            {"name": "auth-service", "language": "go", "score": 0.95},
            {"name": "user-service", "language": "python", "score": 0.82},
        ]
        mock_session = _make_neo4j_session(run_return=mock_records)
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="What language is auth-service?",
            retrieval_path="vector",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            result = await vector_retrieve(state)

        assert len(result["candidates"]) == 2
        assert result["candidates"][0]["name"] == "auth-service"

    @pytest.mark.asyncio
    async def test_respects_max_results(self, base_query_state):
        from orchestrator.app.query_engine import vector_retrieve

        mock_session = _make_neo4j_session(run_return=[{"name": "svc", "score": 0.9}])
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="auth-service",
            max_results=5,
            retrieval_path="vector",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            await vector_retrieve(state)

        call_args = mock_session.run.call_args
        assert "$limit" in call_args[0][0]
        assert call_args.kwargs["limit"] == 5


class TestSingleHopRetrieve:
    @pytest.mark.asyncio
    async def test_returns_vector_and_one_hop_neighbors(self, base_query_state):
        from orchestrator.app.query_engine import single_hop_retrieve

        mock_vector_result = MagicMock()
        mock_vector_result.data.return_value = [{"name": "order-service", "score": 0.95}]
        mock_hop_result = MagicMock()
        mock_hop_result.data.return_value = [
            {"source": "order-service", "rel": "PRODUCES", "target": "orders-topic"}
        ]

        mock_session = _make_neo4j_session(
            run_side_effect=[mock_vector_result, mock_hop_result]
        )
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="What topics does order-service produce to?",
            retrieval_path="single_hop",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            result = await single_hop_retrieve(state)

        assert len(result["candidates"]) == 1
        assert result["candidates"][0]["name"] == "order-service"
        assert len(result["cypher_results"]) == 1
        assert result["cypher_results"][0]["target"] == "orders-topic"
        assert mock_session.run.call_count == 2

    @pytest.mark.asyncio
    async def test_hop_query_uses_vector_names(self, base_query_state):
        from orchestrator.app.query_engine import single_hop_retrieve

        mock_vector_result = MagicMock()
        mock_vector_result.data.return_value = [
            {"name": "svc-a", "score": 0.9},
            {"name": "svc-b", "score": 0.8},
        ]
        mock_hop_result = MagicMock()
        mock_hop_result.data.return_value = [
            {"source": "svc-a", "rel": "CALLS", "target": "svc-c"}
        ]

        mock_session = _make_neo4j_session(
            run_side_effect=[mock_vector_result, mock_hop_result]
        )
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="What does svc-a call?",
            retrieval_path="single_hop",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            result = await single_hop_retrieve(state)

        hop_call_args = mock_session.run.call_args_list[1]
        assert "$names" in hop_call_args[0][0]
        assert hop_call_args.kwargs["names"] == ["svc-a", "svc-b"]


class TestCypherRetrieve:
    @pytest.mark.asyncio
    async def test_generates_and_executes_cypher(self, base_query_state):
        from orchestrator.app.query_engine import cypher_retrieve

        mock_session = _make_neo4j_session(
            run_return=[{"source": "auth", "target": "user", "rel": "CALLS"}]
        )
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="What is the blast radius if auth fails?",
            retrieval_path="cypher",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            new_callable=AsyncMock,
            return_value="MATCH (s:Service)-[:CALLS*]->(t) RETURN s, t",
        ):
            result = await cypher_retrieve(state)

        assert len(result["cypher_results"]) == 1
        assert result["iteration_count"] == 1
        assert result["cypher_query"] != ""

    @pytest.mark.asyncio
    async def test_iterates_on_empty_results_up_to_max(self, base_query_state):
        from orchestrator.app.query_engine import (
            MAX_CYPHER_ITERATIONS,
            cypher_retrieve,
        )

        mock_session = _make_neo4j_session(run_return=[])
        mock_driver = mock_neo4j_driver_with_session(mock_session)
        generate_mock = AsyncMock(return_value="MATCH (n) RETURN n")

        state = _make_state(
            base_query_state,
            query="blast radius if auth fails?",
            retrieval_path="cypher",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            generate_mock,
        ):
            result = await cypher_retrieve(state)

        assert generate_mock.call_count == MAX_CYPHER_ITERATIONS
        assert result["iteration_count"] == MAX_CYPHER_ITERATIONS
        assert result["cypher_results"] == []

    @pytest.mark.asyncio
    async def test_stops_iterating_when_results_found(self, base_query_state):
        from orchestrator.app.query_engine import cypher_retrieve

        empty_result = MagicMock()
        empty_result.data.return_value = []
        good_result = MagicMock()
        good_result.data.return_value = [{"node": "auth"}]

        mock_session = _make_neo4j_session(
            run_side_effect=[empty_result, good_result]
        )
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        cyphers = iter([
            "MATCH (n) RETURN n",
            "MATCH (s:Service)-[:CALLS]->(t) RETURN s, t",
        ])
        generate_mock = AsyncMock(side_effect=lambda *a, **kw: next(cyphers))

        state = _make_state(
            base_query_state,
            query="blast radius if auth fails?",
            retrieval_path="cypher",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            generate_mock,
        ):
            result = await cypher_retrieve(state)

        assert generate_mock.call_count == 2
        assert result["iteration_count"] == 2
        assert len(result["cypher_results"]) == 1


class TestHybridRetrieve:
    @pytest.mark.asyncio
    async def test_prefilters_then_aggregates(self, base_query_state):
        from orchestrator.app.query_engine import hybrid_retrieve

        mock_vector_result = MagicMock()
        mock_vector_result.data.return_value = [{"name": "auth-service", "score": 0.9}]
        mock_agg_result = MagicMock()
        mock_agg_result.data.return_value = [{"service": "auth-service", "dep_count": 5}]

        mock_vector_session = AsyncMock()
        mock_vector_session.run = AsyncMock(return_value=mock_vector_result)
        mock_vector_session.__aenter__ = AsyncMock(return_value=mock_vector_session)
        mock_vector_session.__aexit__ = AsyncMock(return_value=False)

        mock_agg_session = AsyncMock()
        mock_agg_session.run = AsyncMock(return_value=mock_agg_result)
        mock_agg_session.__aenter__ = AsyncMock(return_value=mock_agg_session)
        mock_agg_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(
            side_effect=[mock_vector_session, mock_agg_session]
        )
        mock_driver.close = AsyncMock()

        state = _make_state(
            base_query_state,
            query="Most critical services by dependency count",
            retrieval_path="hybrid",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            new_callable=AsyncMock,
            return_value="MATCH (s:Service)-[:CALLS*]->(t) RETURN s.name, count(t)",
        ):
            result = await hybrid_retrieve(state)

        assert len(result["candidates"]) > 0
        assert len(result["cypher_results"]) > 0

    @pytest.mark.asyncio
    async def test_llm_call_outside_session_scope(self, base_query_state):
        from orchestrator.app.query_engine import hybrid_retrieve

        mock_vector_result = MagicMock()
        mock_vector_result.data.return_value = [{"name": "svc-a", "score": 0.8}]
        mock_agg_result = MagicMock()
        mock_agg_result.data.return_value = [{"total": 3}]

        session_entry_order: list = []

        mock_vector_session = AsyncMock()
        mock_vector_session.run = AsyncMock(return_value=mock_vector_result)
        mock_vector_session.__aenter__ = AsyncMock(return_value=mock_vector_session)

        async def vector_exit(*_args):
            session_entry_order.append("vector_session_closed")
            return False

        mock_vector_session.__aexit__ = vector_exit

        mock_agg_session = AsyncMock()
        mock_agg_session.run = AsyncMock(return_value=mock_agg_result)
        mock_agg_session.__aenter__ = AsyncMock(return_value=mock_agg_session)
        mock_agg_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(
            side_effect=[mock_vector_session, mock_agg_session]
        )
        mock_driver.close = AsyncMock()

        async def fake_generate_cypher(*_args, **_kwargs):
            session_entry_order.append("llm_called")
            return "MATCH (n) RETURN n"

        state = _make_state(
            base_query_state,
            query="aggregate query",
            retrieval_path="hybrid",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            side_effect=fake_generate_cypher,
        ):
            await hybrid_retrieve(state)

        assert session_entry_order.index("vector_session_closed") < \
            session_entry_order.index("llm_called")


class TestSynthesize:
    @pytest.mark.asyncio
    async def test_produces_answer_from_candidates(self, base_query_state):
        from orchestrator.app.query_engine import synthesize_answer

        state = _make_state(
            base_query_state,
            query="What language is auth-service?",
            retrieval_path="vector",
            candidates=[{"name": "auth-service", "language": "go", "score": 0.95}],
        )

        with patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="auth-service is written in Go.",
        ):
            result = await synthesize_answer(state)

        assert result["answer"] != ""
        assert len(result["sources"]) > 0

    @pytest.mark.asyncio
    async def test_returns_fallback_when_context_empty(self, base_query_state):
        from orchestrator.app.query_engine import synthesize_answer

        state = _make_state(
            base_query_state,
            query="What port does ghost-service listen on?",
            retrieval_path="vector",
        )
        result = await synthesize_answer(state)
        assert result["answer"] == "No relevant information found in the knowledge graph."
        assert result["sources"] == []

    @pytest.mark.asyncio
    async def test_includes_cypher_results_as_sources(self, base_query_state):
        from orchestrator.app.query_engine import synthesize_answer

        state = _make_state(
            base_query_state,
            query="blast radius of auth failure",
            retrieval_path="cypher",
            cypher_query="MATCH ...",
            cypher_results=[
                {"source": "auth", "target": "user"},
                {"source": "auth", "target": "order"},
            ],
            iteration_count=1,
        )

        with patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="If auth fails, user and order services are affected.",
        ):
            result = await synthesize_answer(state)

        assert result["answer"] != ""
        assert len(result["sources"]) == 2
