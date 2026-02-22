import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Tuple

from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import END, START, StateGraph
from neo4j import AsyncDriver, AsyncManagedTransaction

from orchestrator.app.access_control import (
    AuthConfigurationError,
    CypherPermissionFilter,
    SecurityPrincipal,
)
from orchestrator.app.config import AuthConfig, ExtractionConfig
from orchestrator.app.cypher_validator import CypherValidationError, validate_cypher_readonly
from orchestrator.app.neo4j_pool import get_driver
from orchestrator.app.observability import QUERY_DURATION, get_tracer
from orchestrator.app.query_classifier import classify_query
from orchestrator.app.query_models import QueryComplexity, QueryState

_ROUTE_MAP = {
    QueryComplexity.ENTITY_LOOKUP: "vector",
    QueryComplexity.SINGLE_HOP: "single_hop",
    QueryComplexity.MULTI_HOP: "cypher",
    QueryComplexity.AGGREGATE: "hybrid",
}

MAX_CYPHER_ITERATIONS = 3

_VECTOR_SEARCH_CYPHER = (
    "CALL db.index.fulltext.queryNodes('service_name_index', $query) "
    "YIELD node, score "
    "RETURN node {.*, score: score} AS result "
    "ORDER BY score DESC LIMIT $limit"
)


def _get_neo4j_driver() -> AsyncDriver:
    return get_driver()


@asynccontextmanager
async def _neo4j_session() -> AsyncIterator[AsyncDriver]:
    yield _get_neo4j_driver()


def _build_acl_filter(
    state: QueryState,
) -> CypherPermissionFilter:
    auth_config = AuthConfig.from_env()
    if auth_config.require_tokens and not auth_config.token_secret:
        raise AuthConfigurationError(
            "server misconfigured: token verification required but no secret set"
        )
    principal = SecurityPrincipal.from_header(
        state.get("authorization", ""),
        token_secret=auth_config.token_secret,
    )
    return CypherPermissionFilter(principal)


def _apply_acl(
    cypher: str, state: QueryState, alias: str = "n",
) -> Tuple[str, Dict[str, str]]:
    acl_filter = _build_acl_filter(state)
    return acl_filter.inject_into_cypher(cypher, alias=alias)


def _build_llm() -> ChatGoogleGenerativeAI:
    config = ExtractionConfig.from_env()
    return ChatGoogleGenerativeAI(
        model=config.model_name,
        google_api_key=config.google_api_key,
    )


async def _generate_cypher(query: str, schema_hint: str = "") -> str:
    llm = _build_llm()
    prompt = (
        "Generate a Neo4j Cypher query to answer this question about a "
        "distributed system knowledge graph.\n\n"
        "Node labels: Service, Database, KafkaTopic, K8sDeployment\n"
        "Relationship types: CALLS, PRODUCES, CONSUMES, DEPLOYED_IN\n"
        f"Schema hint: {schema_hint}\n\n"
        f"Question: {query}\n\n"
        "Return ONLY the Cypher query, no explanation."
    )
    response = await llm.ainvoke(prompt)
    return response.content.strip().strip("`").strip()


async def _llm_synthesize(
    query: str,
    context: List[Dict[str, Any]],
) -> str:
    llm = _build_llm()
    prompt = (
        "You are a distributed systems expert. Answer the following question "
        "using ONLY the provided graph context. Be concise and precise.\n\n"
        f"Question: {query}\n\n"
        f"Graph context:\n{context}\n\n"
        "Answer:"
    )
    response = await llm.ainvoke(prompt)
    return response.content.strip()


def classify_query_node(state: QueryState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.classify") as span:
        start = time.monotonic()
        complexity = classify_query(state["query"])
        span.set_attribute("query.complexity", complexity.value)
        QUERY_DURATION.record(
            (time.monotonic() - start) * 1000, {"node": "classify"}
        )
        return {
            "complexity": complexity,
            "retrieval_path": _ROUTE_MAP[complexity],
        }


def route_query(state: QueryState) -> str:
    path = state.get("retrieval_path", "vector")
    return {
        "vector": "vector_retrieve",
        "single_hop": "single_hop_retrieve",
        "cypher": "cypher_retrieve",
        "hybrid": "hybrid_retrieve",
    }.get(path, "vector_retrieve")


async def vector_retrieve(state: QueryState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.vector_retrieve"):
        start = time.monotonic()
        try:
            async with _neo4j_session() as driver:
                query_text = state["query"]
                limit = state.get("max_results", 10)
                filtered_cypher, acl_params = _apply_acl(
                    _VECTOR_SEARCH_CYPHER, state, alias="node",
                )

                async def _tx_func(
                    tx: AsyncManagedTransaction,
                ) -> list:
                    result = await tx.run(
                        filtered_cypher,
                        query=query_text,
                        limit=limit,
                        **acl_params,
                    )
                    return await result.data()

                async with driver.session() as session:
                    records = await session.execute_read(_tx_func)
                return {"candidates": records}
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "vector_retrieve"}
            )


async def single_hop_retrieve(state: QueryState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.single_hop_retrieve"):
        start = time.monotonic()
        try:
            async with _neo4j_session() as driver:
                vec_cypher, vec_acl = _apply_acl(
                    _VECTOR_SEARCH_CYPHER, state, alias="node",
                )

                async def _vector_tx(tx: AsyncManagedTransaction) -> list:
                    result = await tx.run(
                        vec_cypher,
                        query=state["query"],
                        limit=state.get("max_results", 10),
                        **vec_acl,
                    )
                    return await result.data()

                async with driver.session() as session:
                    candidates = await session.execute_read(_vector_tx)
                    names = [
                        c.get("name", c.get("result", {}).get("name", ""))
                        for c in candidates
                    ]
                    hop_cypher, hop_acl = _apply_acl(
                        "MATCH (n)-[r]-(m) "
                        "WHERE n.name IN $names "
                        "RETURN n.name AS source, type(r) AS rel, "
                        "m.name AS target",
                        state, alias="m",
                    )

                    async def _hop_tx(tx: AsyncManagedTransaction) -> list:
                        result = await tx.run(
                            hop_cypher, names=names, **hop_acl,
                        )
                        return await result.data()

                    hop_records = await session.execute_read(_hop_tx)

                return {
                    "candidates": candidates,
                    "cypher_results": hop_records,
                }
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000,
                {"node": "single_hop_retrieve"},
            )


async def cypher_retrieve(state: QueryState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.cypher_retrieve"):
        start = time.monotonic()
        try:
            async with _neo4j_session() as driver:
                records: list = []
                generated = ""
                prior_attempt = ""
                for iteration in range(1, MAX_CYPHER_ITERATIONS + 1):
                    schema_hint = (
                        f"Previous Cypher returned no results: {prior_attempt}"
                        if prior_attempt else ""
                    )
                    generated = await _generate_cypher(
                        state["query"], schema_hint=schema_hint
                    )
                    try:
                        validated = validate_cypher_readonly(generated)
                    except CypherValidationError:
                        return {
                            "cypher_query": generated,
                            "cypher_results": [],
                            "iteration_count": iteration,
                        }

                    acl_cypher, acl_params = _apply_acl(
                        validated, state,
                    )
                    frozen_acl = tuple(acl_params.items())

                    async def _cypher_tx(
                        tx: AsyncManagedTransaction,
                        _q: str = acl_cypher,
                        _fp: tuple = frozen_acl,
                    ) -> list:
                        result = await tx.run(_q, **dict(_fp))
                        return await result.data()

                    async with driver.session() as session:
                        records = await session.execute_read(_cypher_tx)
                    if records:
                        return {
                            "cypher_query": validated,
                            "cypher_results": records,
                            "iteration_count": iteration,
                        }
                    prior_attempt = validated
                return {
                    "cypher_query": generated,
                    "cypher_results": records,
                    "iteration_count": MAX_CYPHER_ITERATIONS,
                }
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "cypher_retrieve"}
            )


async def hybrid_retrieve(state: QueryState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.hybrid_retrieve"):
        start = time.monotonic()
        try:
            candidates = await _hybrid_vector_phase(state)
            return await _hybrid_aggregation_phase(state, candidates)
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "hybrid_retrieve"}
            )


async def _hybrid_vector_phase(state: QueryState) -> list:
    async with _neo4j_session() as driver:
        vec_cypher, vec_acl = _apply_acl(
            _VECTOR_SEARCH_CYPHER, state, alias="node",
        )

        async def _vector_tx(tx: AsyncManagedTransaction) -> list:
            result = await tx.run(
                vec_cypher,
                query=state["query"],
                limit=state.get("max_results", 10),
                **vec_acl,
            )
            return await result.data()

        async with driver.session() as session:
            return await session.execute_read(_vector_tx)


async def _hybrid_aggregation_phase(
    state: QueryState, candidates: list,
) -> dict:
    agg_cypher = await _generate_cypher(
        state["query"],
        schema_hint=f"Pre-filtered candidates: {candidates}",
    )

    try:
        validated_agg = validate_cypher_readonly(agg_cypher)
    except CypherValidationError:
        return {
            "candidates": candidates,
            "cypher_query": agg_cypher,
            "cypher_results": [],
            "iteration_count": 1,
        }

    agg_acl_cypher, agg_acl_params = _apply_acl(validated_agg, state)

    async with _neo4j_session() as driver:
        async def _agg_tx(tx: AsyncManagedTransaction) -> list:
            result = await tx.run(agg_acl_cypher, **agg_acl_params)
            return await result.data()

        async with driver.session() as session:
            agg_records = await session.execute_read(_agg_tx)

    return {
        "candidates": candidates,
        "cypher_query": validated_agg,
        "cypher_results": agg_records,
        "iteration_count": 1,
    }


async def synthesize_answer(state: QueryState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.synthesize"):
        start = time.monotonic()
        result = await _do_synthesize(state)
        QUERY_DURATION.record(
            (time.monotonic() - start) * 1000, {"node": "synthesize"}
        )
        return result


async def _do_synthesize(state: QueryState) -> dict:
    context: List[Dict[str, Any]] = []
    if state.get("candidates"):
        context.extend(state["candidates"])
    if state.get("cypher_results"):
        context.extend(state["cypher_results"])

    if not context:
        return {
            "answer": "No relevant information found in the knowledge graph.",
            "sources": [],
        }

    answer = await _llm_synthesize(state["query"], context)
    return {"answer": answer, "sources": context}


builder = StateGraph(QueryState)

builder.add_node("classify", classify_query_node)
builder.add_node("vector_retrieve", vector_retrieve)
builder.add_node("single_hop_retrieve", single_hop_retrieve)
builder.add_node("cypher_retrieve", cypher_retrieve)
builder.add_node("hybrid_retrieve", hybrid_retrieve)
builder.add_node("synthesize", synthesize_answer)

builder.add_edge(START, "classify")
builder.add_conditional_edges(
    "classify",
    route_query,
    {
        "vector_retrieve": "vector_retrieve",
        "single_hop_retrieve": "single_hop_retrieve",
        "cypher_retrieve": "cypher_retrieve",
        "hybrid_retrieve": "hybrid_retrieve",
    },
)
builder.add_edge("vector_retrieve", "synthesize")
builder.add_edge("single_hop_retrieve", "synthesize")
builder.add_edge("cypher_retrieve", "synthesize")
builder.add_edge("hybrid_retrieve", "synthesize")
builder.add_edge("synthesize", END)

query_graph = builder.compile()
