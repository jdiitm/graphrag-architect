import pytest

from orchestrator.app.query_models import QueryComplexity


class TestQueryRouterImportable:
    def test_query_router_importable(self) -> None:
        from orchestrator.app.query_router import (
            _ROUTE_MAP,
            classify_query_node,
            route_query,
        )
        assert callable(classify_query_node)
        assert callable(route_query)
        assert isinstance(_ROUTE_MAP, dict)


class TestQueryRetrieverImportable:
    def test_query_retriever_importable(self) -> None:
        from orchestrator.app.query_retriever import (
            cypher_retrieve,
            hybrid_retrieve,
            single_hop_retrieve,
            vector_retrieve,
        )
        assert callable(vector_retrieve)
        assert callable(single_hop_retrieve)
        assert callable(cypher_retrieve)
        assert callable(hybrid_retrieve)


class TestQuerySynthesizerImportable:
    def test_query_synthesizer_importable(self) -> None:
        from orchestrator.app.query_synthesizer import (
            _llm_synthesize,
            synthesize_answer,
        )
        assert callable(synthesize_answer)
        assert callable(_llm_synthesize)


class TestBackwardCompatImports:
    def test_router_symbols_from_engine(self) -> None:
        from orchestrator.app.query_engine import classify_query_node, route_query
        assert callable(classify_query_node)
        assert callable(route_query)

    def test_retriever_symbols_from_engine(self) -> None:
        from orchestrator.app.query_engine import (
            _CB_EMBEDDING_GLOBAL,
            _SEMANTIC_CACHE,
            _SUBGRAPH_CACHE,
            _build_acl_filter,
            _embed_query,
            _execute_sandboxed_read,
            _neo4j_session,
            cypher_retrieve,
            hybrid_retrieve,
            single_hop_retrieve,
            vector_retrieve,
        )
        assert callable(vector_retrieve)
        assert callable(single_hop_retrieve)
        assert callable(cypher_retrieve)
        assert callable(hybrid_retrieve)
        assert callable(_embed_query)
        assert _neo4j_session is not None
        assert callable(_build_acl_filter)
        assert callable(_execute_sandboxed_read)
        assert _SUBGRAPH_CACHE is not None
        assert _SEMANTIC_CACHE is not None
        assert _CB_EMBEDDING_GLOBAL is not None

    def test_synthesizer_symbols_from_engine(self) -> None:
        from orchestrator.app.query_engine import (
            _CB_LLM_GLOBAL,
            _do_synthesize,
            _llm_synthesize,
            _raw_llm_synthesize,
            synthesize_answer,
        )
        assert callable(synthesize_answer)
        assert callable(_llm_synthesize)
        assert callable(_raw_llm_synthesize)
        assert _CB_LLM_GLOBAL is not None
        assert callable(_do_synthesize)

    def test_engine_own_symbols(self) -> None:
        from orchestrator.app.query_engine import (
            _EVAL_STORE,
            evaluate_response,
            get_eval_store,
            query_graph,
        )
        assert callable(evaluate_response)
        assert callable(get_eval_store)
        assert query_graph is not None
        assert _EVAL_STORE is not None


class TestRouterClassifyReturnsCorrectRoute:
    def test_route_map_keys(self) -> None:
        from orchestrator.app.query_router import _ROUTE_MAP
        assert set(_ROUTE_MAP.keys()) == {
            QueryComplexity.ENTITY_LOOKUP,
            QueryComplexity.SINGLE_HOP,
            QueryComplexity.MULTI_HOP,
            QueryComplexity.AGGREGATE,
        }

    def test_route_map_values(self) -> None:
        from orchestrator.app.query_router import _ROUTE_MAP
        assert _ROUTE_MAP[QueryComplexity.ENTITY_LOOKUP] == "vector"
        assert _ROUTE_MAP[QueryComplexity.SINGLE_HOP] == "single_hop"
        assert _ROUTE_MAP[QueryComplexity.MULTI_HOP] == "cypher"
        assert _ROUTE_MAP[QueryComplexity.AGGREGATE] == "hybrid"


class TestRouteQueryMapsPaths:
    def test_maps_vector(self) -> None:
        from orchestrator.app.query_router import route_query
        assert route_query({"query": "x", "retrieval_path": "vector"}) == "vector_retrieve"

    def test_maps_single_hop(self) -> None:
        from orchestrator.app.query_router import route_query
        assert route_query({"query": "x", "retrieval_path": "single_hop"}) == "single_hop_retrieve"

    def test_maps_cypher(self) -> None:
        from orchestrator.app.query_router import route_query
        assert route_query({"query": "x", "retrieval_path": "cypher"}) == "cypher_retrieve"

    def test_maps_hybrid(self) -> None:
        from orchestrator.app.query_router import route_query
        assert route_query({"query": "x", "retrieval_path": "hybrid"}) == "hybrid_retrieve"

    def test_defaults_to_vector(self) -> None:
        from orchestrator.app.query_router import route_query
        assert route_query({"query": "x"}) == "vector_retrieve"


class TestDagStillCompiles:
    def test_dag_compiles(self) -> None:
        from orchestrator.app.query_engine import query_graph
        assert query_graph is not None
        assert hasattr(query_graph, "invoke")
