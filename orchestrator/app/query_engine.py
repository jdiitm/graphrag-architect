from typing import Any, Dict, List

from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import END, START, StateGraph
from neo4j import AsyncGraphDatabase

from orchestrator.app.config import ExtractionConfig, Neo4jConfig
from orchestrator.app.query_classifier import classify_query
from orchestrator.app.query_models import QueryComplexity, QueryState

_ROUTE_MAP = {
    QueryComplexity.ENTITY_LOOKUP: "vector",
    QueryComplexity.SINGLE_HOP: "vector",
    QueryComplexity.MULTI_HOP: "cypher",
    QueryComplexity.AGGREGATE: "hybrid",
}

MAX_CYPHER_ITERATIONS = 3


def _get_neo4j_driver():
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
    complexity = classify_query(state["query"])
    return {
        "complexity": complexity,
        "retrieval_path": _ROUTE_MAP[complexity],
    }


def route_query(state: QueryState) -> str:
    path = state.get("retrieval_path", "vector")
    return {
        "vector": "vector_retrieve",
        "cypher": "cypher_retrieve",
        "hybrid": "hybrid_retrieve",
    }.get(path, "vector_retrieve")


async def vector_retrieve(state: QueryState) -> dict:
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
        await driver.close()


async def cypher_retrieve(state: QueryState) -> dict:
    generated = await _generate_cypher(state["query"])
    driver = _get_neo4j_driver()
    try:
        async with driver.session() as session:
            result = await session.run(generated)
            records = result.data()
        return {
            "cypher_query": generated,
            "cypher_results": records,
            "iteration_count": state.get("iteration_count", 0) + 1,
        }
    finally:
        await driver.close()


async def hybrid_retrieve(state: QueryState) -> dict:
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
            agg_result = await session.run(agg_cypher)
            agg_records = agg_result.data()

        return {
            "candidates": candidates,
            "cypher_query": agg_cypher,
            "cypher_results": agg_records,
            "iteration_count": 1,
        }
    finally:
        await driver.close()


async def synthesize_answer(state: QueryState) -> dict:
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
builder.add_node("cypher_retrieve", cypher_retrieve)
builder.add_node("hybrid_retrieve", hybrid_retrieve)
builder.add_node("synthesize", synthesize_answer)

builder.add_edge(START, "classify")
builder.add_conditional_edges(
    "classify",
    route_query,
    {
        "vector_retrieve": "vector_retrieve",
        "cypher_retrieve": "cypher_retrieve",
        "hybrid_retrieve": "hybrid_retrieve",
    },
)
builder.add_edge("vector_retrieve", "synthesize")
builder.add_edge("cypher_retrieve", "synthesize")
builder.add_edge("hybrid_retrieve", "synthesize")
builder.add_edge("synthesize", END)

query_graph = builder.compile()
