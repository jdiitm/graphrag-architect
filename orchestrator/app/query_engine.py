import logging
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import END, START, StateGraph
from neo4j import AsyncDriver, AsyncManagedTransaction

from orchestrator.app.access_control import (
    AuthConfigurationError,
    CypherPermissionFilter,
    SecurityPrincipal,
)
from orchestrator.app.config import AuthConfig, EmbeddingConfig, ExtractionConfig, RAGEvalConfig
from orchestrator.app.prompt_sanitizer import sanitize_query_input
from orchestrator.app.cypher_validator import CypherValidationError, validate_cypher_readonly
from orchestrator.app.neo4j_pool import get_driver
from orchestrator.app.observability import QUERY_DURATION, get_tracer
from orchestrator.app.query_classifier import classify_query
from orchestrator.app.query_models import QueryComplexity, QueryState
from orchestrator.app.rag_evaluator import RAGEvaluator

try:
    import openai as _openai_module
except ImportError:
    _openai_module = None

_query_logger = logging.getLogger(__name__)

_ROUTE_MAP = {
    QueryComplexity.ENTITY_LOOKUP: "vector",
    QueryComplexity.SINGLE_HOP: "single_hop",
    QueryComplexity.MULTI_HOP: "cypher",
    QueryComplexity.AGGREGATE: "hybrid",
}

MAX_CYPHER_ITERATIONS = 3

_VECTOR_SEARCH_CYPHER = (
    "CALL db.index.vector.queryNodes('service_embedding_index', $limit, $query_embedding) "
    "YIELD node, score "
    "RETURN node {.*, score: score} AS result "
    "ORDER BY score DESC"
)

_FULLTEXT_FALLBACK_CYPHER = (
    "CALL db.index.fulltext.queryNodes('service_name_index', $query) "
    "YIELD node, score "
    "RETURN node {.*, score: score} AS result "
    "ORDER BY score DESC LIMIT $limit"
)


async def _embed_query(text: str) -> Optional[List[float]]:
    try:
        cfg = EmbeddingConfig.from_env()
        if cfg.provider == "openai" and _openai_module is not None:
            client = _openai_module.AsyncOpenAI()
            response = await client.embeddings.create(
                input=[text], model=cfg.model_name
            )
            return response.data[0].embedding
    except Exception:
        _query_logger.debug("Embedding unavailable, falling back to fulltext")
    return None


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
        require_verification=auth_config.require_tokens,
    )
    return CypherPermissionFilter(
        principal,
        default_deny_untagged=auth_config.default_deny_untagged,
    )


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
    sanitized = sanitize_query_input(query)
    prompt = (
        "Generate a Neo4j Cypher query to answer this question about a "
        "distributed system knowledge graph.\n\n"
        "Node labels: Service, Database, KafkaTopic, K8sDeployment\n"
        "Relationship types: CALLS, PRODUCES, CONSUMES, DEPLOYED_IN\n"
        f"Schema hint: {schema_hint}\n\n"
        f"Question: {sanitized}\n\n"
        "Return ONLY the Cypher query, no explanation."
    )
    response = await llm.ainvoke(prompt)
    return response.content.strip().strip("`").strip()


async def _llm_synthesize(
    query: str,
    context: List[Dict[str, Any]],
) -> str:
    llm = _build_llm()
    sanitized = sanitize_query_input(query)
    prompt = (
        "You are a distributed systems expert. Answer the following question "
        "using ONLY the provided graph context. Be concise and precise.\n\n"
        f"Question: {sanitized}\n\n"
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
                query_embedding = await _embed_query(query_text)
                use_vector = query_embedding is not None

                if use_vector:
                    cypher = _VECTOR_SEARCH_CYPHER
                else:
                    cypher = _FULLTEXT_FALLBACK_CYPHER

                filtered_cypher, acl_params = _apply_acl(
                    cypher, state, alias="node",
                )

                async def _tx_func(
                    tx: AsyncManagedTransaction,
                ) -> list:
                    params: Dict[str, Any] = {**acl_params, "limit": limit}
                    if use_vector:
                        params["query_embedding"] = query_embedding
                    else:
                        params["query"] = query_text
                    result = await tx.run(filtered_cypher, **params)
                    return await result.data()

                async with driver.session() as session:
                    records = await session.execute_read(_tx_func)
                return {"candidates": records}
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "vector_retrieve"}
            )


async def _fetch_candidates(
    driver: AsyncDriver, state: QueryState,
) -> list:
    query_embedding = await _embed_query(state["query"])
    use_vector = query_embedding is not None
    base_cypher = _VECTOR_SEARCH_CYPHER if use_vector else _FULLTEXT_FALLBACK_CYPHER
    vec_cypher, vec_acl = _apply_acl(base_cypher, state, alias="node")

    async def _vector_tx(tx: AsyncManagedTransaction) -> list:
        params: Dict[str, Any] = {
            **vec_acl, "limit": state.get("max_results", 10),
        }
        if use_vector:
            params["query_embedding"] = query_embedding
        else:
            params["query"] = state["query"]
        result = await tx.run(vec_cypher, **params)
        return await result.data()

    async with driver.session() as session:
        return await session.execute_read(_vector_tx)


async def single_hop_retrieve(state: QueryState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.single_hop_retrieve"):
        start = time.monotonic()
        try:
            async with _neo4j_session() as driver:
                candidates = await _fetch_candidates(driver, state)
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

                async with driver.session() as session:
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
        query_embedding = await _embed_query(state["query"])
        use_vector = query_embedding is not None
        base_cypher = _VECTOR_SEARCH_CYPHER if use_vector else _FULLTEXT_FALLBACK_CYPHER
        vec_cypher, vec_acl = _apply_acl(
            base_cypher, state, alias="node",
        )

        async def _vector_tx(tx: AsyncManagedTransaction) -> list:
            params: Dict[str, Any] = {
                **vec_acl,
                "limit": state.get("max_results", 10),
            }
            if use_vector:
                params["query_embedding"] = query_embedding
            else:
                params["query"] = state["query"]
            result = await tx.run(vec_cypher, **params)
            return await result.data()

        async with driver.session() as session:
            return await session.execute_read(_vector_tx)


async def _hybrid_aggregation_phase(
    state: QueryState, candidates: list,
) -> dict:
    agg_cypher = await _generate_cypher(
        state["query"],
        schema_hint=f"Pre-filtered candidates (count={len(candidates)})",
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


async def evaluate_response(state: QueryState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.evaluate"):
        start = time.monotonic()
        try:
            eval_config = RAGEvalConfig.from_env()
            if not eval_config.enable_evaluation:
                return {"evaluation_score": -1.0, "retrieval_quality": "skipped"}

            query_embedding = await _embed_query(state["query"])
            if query_embedding is None:
                return {"evaluation_score": -1.0, "retrieval_quality": "no_embedding"}

            context_embeddings: List[List[float]] = []
            for source in state.get("sources", []):
                if isinstance(source, dict):
                    emb = source.get("embedding")
                    if isinstance(emb, list):
                        context_embeddings.append(emb)

            evaluator = RAGEvaluator(
                low_relevance_threshold=eval_config.low_relevance_threshold,
            )
            result = evaluator.evaluate(
                query=state["query"],
                query_embedding=query_embedding,
                context_embeddings=context_embeddings,
                retrieval_path=state.get("retrieval_path", "vector"),
            )
            quality = "low" if evaluator.is_low_relevance(result) else "adequate"
            if result.score >= 0.7:
                quality = "high"

            _query_logger.info(
                "RAG evaluation: score=%.3f quality=%s path=%s contexts=%d",
                result.score,
                quality,
                result.retrieval_path,
                result.context_count,
            )
            return {"evaluation_score": result.score, "retrieval_quality": quality}
        except Exception:
            _query_logger.debug("RAG evaluation failed, continuing without score")
            return {"evaluation_score": -1.0, "retrieval_quality": "error"}
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "evaluate"}
            )


builder = StateGraph(QueryState)

builder.add_node("classify", classify_query_node)
builder.add_node("vector_retrieve", vector_retrieve)
builder.add_node("single_hop_retrieve", single_hop_retrieve)
builder.add_node("cypher_retrieve", cypher_retrieve)
builder.add_node("hybrid_retrieve", hybrid_retrieve)
builder.add_node("synthesize", synthesize_answer)
builder.add_node("evaluate", evaluate_response)

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
builder.add_edge("synthesize", "evaluate")
builder.add_edge("evaluate", END)

query_graph = builder.compile()
