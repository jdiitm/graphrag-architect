import time
from typing import Any, Dict, List

from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import END, START, StateGraph
from neo4j import AsyncDriver, AsyncGraphDatabase

from orchestrator.app.config import ExtractionConfig, Neo4jConfig
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


def _get_neo4j_driver() -> AsyncDriver:
    config = Neo4jConfig.from_env()
    return AsyncGraphDatabase.driver(
        config.uri, auth=(config.username, config.password)
    )


async def _generate_cypher(query: str, schema_hint: str = "") -> str:
    config = ExtractionConfig.from_env()
    llm = ChatGoogleGenerativeAI(
        model=config.model_name,
        google_api_key=config.google_api_key,
    )
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
    config = ExtractionConfig.from_env()
    llm = ChatGoogleGenerativeAI(
        model=config.model_name,
        google_api_key=config.google_api_key,
    )
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
        driver = _get_neo4j_driver()
        try:
            query_text = state["query"]
            limit = state.get("max_results", 10)
            cypher = (
                "CALL db.index.fulltext.queryNodes('service_name_index', $query) "
                "YIELD node, score "
                "RETURN node {.*, score: score} AS result "
                "ORDER BY score DESC LIMIT $limit"
            )
            async with driver.session() as session:
                result = await session.run(cypher, query=query_text, limit=limit)
                records = result.data()
            return {"candidates": records}
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "vector_retrieve"}
            )
            await driver.close()


async def single_hop_retrieve(state: QueryState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.single_hop_retrieve"):
        start = time.monotonic()
        driver = _get_neo4j_driver()
        try:
            query_text = state["query"]
            limit = state.get("max_results", 10)
            vector_cypher = (
                "CALL db.index.fulltext.queryNodes('service_name_index', $query) "
                "YIELD node, score "
                "RETURN node {.*, score: score} AS result "
                "ORDER BY score DESC LIMIT $limit"
            )
            async with driver.session() as session:
                vector_result = await session.run(
                    vector_cypher, query=query_text, limit=limit
                )
                candidates = vector_result.data()
                names = [c.get("name", c.get("result", {}).get("name", ""))
                         for c in candidates]

                hop_cypher = (
                    "MATCH (n)-[r]-(m) "
                    "WHERE n.name IN $names "
                    "RETURN n.name AS source, type(r) AS rel, m.name AS target"
                )
                hop_result = await session.run(hop_cypher, names=names)
                hop_records = hop_result.data()

            return {
                "candidates": candidates,
                "cypher_results": hop_records,
            }
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000,
                {"node": "single_hop_retrieve"},
            )
            await driver.close()


async def cypher_retrieve(state: QueryState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.cypher_retrieve"):
        start = time.monotonic()
        driver = _get_neo4j_driver()
        try:
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
                async with driver.session() as session:
                    result = await session.run(generated)
                    records = result.data()
                if records:
                    return {
                        "cypher_query": generated,
                        "cypher_results": records,
                        "iteration_count": iteration,
                    }
                prior_attempt = generated
            return {
                "cypher_query": generated,
                "cypher_results": records,
                "iteration_count": MAX_CYPHER_ITERATIONS,
            }
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "cypher_retrieve"}
            )
            await driver.close()


async def hybrid_retrieve(state: QueryState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.hybrid_retrieve"):
        start = time.monotonic()
        driver = _get_neo4j_driver()
        try:
            query_text = state["query"]
            limit = state.get("max_results", 10)
            vector_cypher = (
                "CALL db.index.fulltext.queryNodes('service_name_index', $query) "
                "YIELD node, score "
                "RETURN node {.*, score: score} AS result "
                "ORDER BY score DESC LIMIT $limit"
            )
            async with driver.session() as session:
                vector_result = await session.run(
                    vector_cypher, query=query_text, limit=limit
                )
                candidates = vector_result.data()

            agg_cypher = await _generate_cypher(
                state["query"],
                schema_hint=f"Pre-filtered candidates: {candidates}",
            )

            async with driver.session() as session:
                agg_result = await session.run(agg_cypher)
                agg_records = agg_result.data()

            return {
                "candidates": candidates,
                "cypher_query": agg_cypher,
                "cypher_results": agg_records,
                "iteration_count": 1,
            }
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "hybrid_retrieve"}
            )
            await driver.close()


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
