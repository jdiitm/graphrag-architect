from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.query_models import QueryComplexity, QueryState


class TestClassifyNode:
    def test_sets_entity_lookup(self):
        from orchestrator.app.query_engine import classify_query_node

        state: QueryState = {
            "query": "What language is auth-service?",
            "max_results": 10,
            "complexity": QueryComplexity.ENTITY_LOOKUP,
            "retrieval_path": "",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        result = classify_query_node(state)
        assert result["complexity"] == QueryComplexity.ENTITY_LOOKUP
        assert result["retrieval_path"] == "vector"

    def test_sets_multi_hop(self):
        from orchestrator.app.query_engine import classify_query_node

        state: QueryState = {
            "query": "What is the blast radius if auth fails?",
            "max_results": 10,
            "complexity": QueryComplexity.ENTITY_LOOKUP,
            "retrieval_path": "",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        result = classify_query_node(state)
        assert result["complexity"] == QueryComplexity.MULTI_HOP
        assert result["retrieval_path"] == "cypher"


class TestRouteQuery:
    def test_routes_entity_lookup_to_vector(self):
        from orchestrator.app.query_engine import route_query

        state: QueryState = {
            "query": "",
            "max_results": 10,
            "complexity": QueryComplexity.ENTITY_LOOKUP,
            "retrieval_path": "vector",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        assert route_query(state) == "vector_retrieve"

    def test_routes_single_hop_to_vector(self):
        from orchestrator.app.query_engine import route_query

        state: QueryState = {
            "query": "",
            "max_results": 10,
            "complexity": QueryComplexity.SINGLE_HOP,
            "retrieval_path": "vector",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        assert route_query(state) == "vector_retrieve"

    def test_routes_multi_hop_to_cypher(self):
        from orchestrator.app.query_engine import route_query

        state: QueryState = {
            "query": "",
            "max_results": 10,
            "complexity": QueryComplexity.MULTI_HOP,
            "retrieval_path": "cypher",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        assert route_query(state) == "cypher_retrieve"

    def test_routes_aggregate_to_hybrid(self):
        from orchestrator.app.query_engine import route_query

        state: QueryState = {
            "query": "",
            "max_results": 10,
            "complexity": QueryComplexity.AGGREGATE,
            "retrieval_path": "hybrid",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        assert route_query(state) == "hybrid_retrieve"


def _mock_driver_with_session(mock_session):
    mock_driver = MagicMock()
    mock_driver.session.return_value = mock_session
    mock_driver.close = AsyncMock()
    return mock_driver


class TestVectorRetrieve:
    @pytest.mark.asyncio
    async def test_returns_candidates_from_neo4j(self):
        from orchestrator.app.query_engine import vector_retrieve

        mock_records = [
            {"name": "auth-service", "language": "go", "score": 0.95},
            {"name": "user-service", "language": "python", "score": 0.82},
        ]
        mock_result = MagicMock()
        mock_result.data.return_value = mock_records

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = _mock_driver_with_session(mock_session)

        state: QueryState = {
            "query": "What language is auth-service?",
            "max_results": 10,
            "complexity": QueryComplexity.ENTITY_LOOKUP,
            "retrieval_path": "vector",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            result = await vector_retrieve(state)

        assert len(result["candidates"]) == 2
        assert result["candidates"][0]["name"] == "auth-service"

    @pytest.mark.asyncio
    async def test_respects_max_results(self):
        from orchestrator.app.query_engine import vector_retrieve

        mock_result = MagicMock()
        mock_result.data.return_value = [{"name": "svc", "score": 0.9}]

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = _mock_driver_with_session(mock_session)

        state: QueryState = {
            "query": "auth-service",
            "max_results": 5,
            "complexity": QueryComplexity.ENTITY_LOOKUP,
            "retrieval_path": "vector",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            await vector_retrieve(state)

        call_args = mock_session.run.call_args
        assert "$limit" in call_args[0][0] or "limit" in str(call_args)


class TestCypherRetrieve:
    @pytest.mark.asyncio
    async def test_generates_and_executes_cypher(self):
        from orchestrator.app.query_engine import cypher_retrieve

        mock_result = MagicMock()
        mock_result.data.return_value = [
            {"source": "auth", "target": "user", "rel": "CALLS"}
        ]

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = _mock_driver_with_session(mock_session)

        state: QueryState = {
            "query": "What is the blast radius if auth fails?",
            "max_results": 10,
            "complexity": QueryComplexity.MULTI_HOP,
            "retrieval_path": "cypher",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }

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


class TestHybridRetrieve:
    @pytest.mark.asyncio
    async def test_prefilters_then_aggregates(self):
        from orchestrator.app.query_engine import hybrid_retrieve

        vector_records = [{"name": "auth-service", "score": 0.9}]
        agg_records = [{"service": "auth-service", "dep_count": 5}]

        mock_vector_result = MagicMock()
        mock_vector_result.data.return_value = vector_records

        mock_agg_result = MagicMock()
        mock_agg_result.data.return_value = agg_records

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(
            side_effect=[mock_vector_result, mock_agg_result]
        )
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = _mock_driver_with_session(mock_session)

        state: QueryState = {
            "query": "Most critical services by dependency count",
            "max_results": 10,
            "complexity": QueryComplexity.AGGREGATE,
            "retrieval_path": "hybrid",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }

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


class TestSynthesize:
    @pytest.mark.asyncio
    async def test_produces_answer_from_candidates(self):
        from orchestrator.app.query_engine import synthesize_answer

        state: QueryState = {
            "query": "What language is auth-service?",
            "max_results": 10,
            "complexity": QueryComplexity.ENTITY_LOOKUP,
            "retrieval_path": "vector",
            "candidates": [
                {"name": "auth-service", "language": "go", "score": 0.95}
            ],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }

        with patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="auth-service is written in Go.",
        ):
            result = await synthesize_answer(state)

        assert result["answer"] != ""
        assert len(result["sources"]) > 0

    @pytest.mark.asyncio
    async def test_includes_cypher_results_as_sources(self):
        from orchestrator.app.query_engine import synthesize_answer

        state: QueryState = {
            "query": "blast radius of auth failure",
            "max_results": 10,
            "complexity": QueryComplexity.MULTI_HOP,
            "retrieval_path": "cypher",
            "candidates": [],
            "cypher_query": "MATCH ...",
            "cypher_results": [
                {"source": "auth", "target": "user"},
                {"source": "auth", "target": "order"},
            ],
            "iteration_count": 1,
            "answer": "",
            "sources": [],
        }

        with patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="If auth fails, user and order services are affected.",
        ):
            result = await synthesize_answer(state)

        assert result["answer"] != ""
        assert len(result["sources"]) == 2
