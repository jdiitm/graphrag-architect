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
from orchestrator.app.cypher_sandbox import (
    SandboxedQueryExecutor,
)
from orchestrator.app.context_manager import (
    TokenBudget,
    format_context_for_prompt,
    truncate_context,
)
from orchestrator.app.prompt_sanitizer import sanitize_query_input
from orchestrator.app.reranker import BM25Reranker
from orchestrator.app.cypher_validator import CypherValidationError, validate_cypher_readonly
from orchestrator.app.query_templates import (
    TemplateCatalog,
    dynamic_cypher_allowed,
    match_template,
)
from orchestrator.app.subgraph_cache import (
    SubgraphCache,
    default_cache_maxsize,
    normalize_cypher,
)
from orchestrator.app.config import VectorStoreConfig
from orchestrator.app.vector_store import SearchResult, create_vector_store
from orchestrator.app.neo4j_pool import get_driver, get_query_timeout
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

_SANDBOX = SandboxedQueryExecutor()
_TEMPLATE_CATALOG = TemplateCatalog()
_SUBGRAPH_CACHE = SubgraphCache(maxsize=default_cache_maxsize())

_VS_CFG = VectorStoreConfig.from_env()
_VECTOR_STORE = create_vector_store(
    backend=_VS_CFG.backend, url=_VS_CFG.qdrant_url, api_key=_VS_CFG.qdrant_api_key,
)

_VECTOR_COLLECTION = "service_embeddings"


def _sandbox_inject_limit(cypher: str) -> str:
    return _SANDBOX.inject_limit(cypher)



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


_EMBEDDING_STATE: Dict[str, Any] = {"cfg": None, "client": None}


def _get_embedding_resources() -> Tuple[Optional[EmbeddingConfig], Any]:
    if _EMBEDDING_STATE["cfg"] is None:
        _EMBEDDING_STATE["cfg"] = EmbeddingConfig.from_env()
    cfg = _EMBEDDING_STATE["cfg"]
    if (
        _EMBEDDING_STATE["client"] is None
        and cfg.provider == "openai"
        and _openai_module is not None
    ):
        _EMBEDDING_STATE["client"] = _openai_module.AsyncOpenAI()
    return cfg, _EMBEDDING_STATE["client"]


async def _embed_query(text: str) -> Optional[List[float]]:
    try:
        cfg, client = _get_embedding_resources()
        if cfg is not None and cfg.provider == "openai" and client is not None:
            response = await client.embeddings.create(
                input=[text], model=cfg.model_name
            )
            return response.data[0].embedding
    except Exception:
        _query_logger.warning(
            "Embedding unavailable, falling back to fulltext", exc_info=True,
        )
    return None


def _get_neo4j_driver() -> AsyncDriver:
    return get_driver()


def _get_query_timeout() -> float:
    return get_query_timeout()


def _get_hop_edge_limit() -> int:
    import os
    raw = os.environ.get("HOP_EDGE_LIMIT", "500")
    try:
        return int(raw)
    except ValueError:
        return 500


def _search_results_to_dicts(results: List[SearchResult]) -> List[Dict[str, Any]]:
    return [{**r.metadata, "score": r.score} for r in results]


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
    formatted_context = format_context_for_prompt(context)
    prompt = (
        "You are a distributed systems expert. Answer the following question "
        "using ONLY the provided graph context. Be concise and precise.\n\n"
        f"Question: {sanitized}\n\n"
        f"Graph context:\n{formatted_context}\n\n"
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
                candidates = await _fetch_candidates(driver, state)
                return {"candidates": candidates}
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "vector_retrieve"}
            )


async def _fetch_candidates(
    driver: AsyncDriver, state: QueryState,
) -> list:
    query_embedding = await _embed_query(state["query"])
    limit = state.get("max_results", 10)
    tenant_id = state.get("tenant_id", "")

    if query_embedding is not None:
        if tenant_id:
            results = await _VECTOR_STORE.search_with_tenant(
                _VECTOR_COLLECTION, query_embedding,
                tenant_id=tenant_id, limit=limit,
            )
        else:
            results = await _VECTOR_STORE.search(
                _VECTOR_COLLECTION, query_embedding, limit=limit,
            )
        return _search_results_to_dicts(results)

    base_cypher = _FULLTEXT_FALLBACK_CYPHER
    vec_cypher, vec_acl = _apply_acl(base_cypher, state, alias="node")

    async def _vector_tx(tx: AsyncManagedTransaction) -> list:
        params: Dict[str, Any] = {
            **vec_acl, "limit": limit, "query": state["query"],
        }
        result = await tx.run(vec_cypher, **params)
        return await result.data()

    timeout = _get_query_timeout()
    async with driver.session() as session:
        return await session.execute_read(_vector_tx, timeout=timeout)


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
                hop_limit = _get_hop_edge_limit()
                hop_cypher, hop_acl = _apply_acl(
                    "MATCH (n)-[r]-(m) "
                    "WHERE n.name IN $names "
                    "AND r.tombstoned_at IS NULL "
                    "RETURN n.name AS source, type(r) AS rel, "
                    "m.name AS target "
                    "LIMIT $hop_limit",
                    state, alias="m",
                )

                async def _hop_tx(tx: AsyncManagedTransaction) -> list:
                    result = await tx.run(
                        hop_cypher, names=names, hop_limit=hop_limit,
                        **hop_acl,
                    )
                    return await result.data()

                timeout = _get_query_timeout()
                async with driver.session() as session:
                    hop_records = await session.execute_read(
                        _hop_tx, timeout=timeout,
                    )

                return {
                    "candidates": candidates,
                    "cypher_results": hop_records,
                }
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000,
                {"node": "single_hop_retrieve"},
            )


async def _execute_sandboxed_read(
    driver: AsyncDriver,
    cypher: str,
    acl_params: Dict[str, str],
) -> Optional[list]:
    cache_key = normalize_cypher(cypher) + "|" + str(sorted(acl_params.items()))
    cached = _SUBGRAPH_CACHE.get(cache_key)
    if cached is not None:
        return cached

    sandboxed = _sandbox_inject_limit(cypher)
    frozen = tuple(acl_params.items())
    async with driver.session() as session:
        async def _tx(
            tx: AsyncManagedTransaction,
            _q: str = sandboxed,
            _fp: tuple = frozen,
        ) -> list:
            result = await tx.run(_q, **dict(_fp))
            return await result.data()

        timeout = _get_query_timeout()
        records = await session.execute_read(_tx, timeout=timeout)

    if records:
        _SUBGRAPH_CACHE.put(cache_key, records)
    return records


async def _try_template_match(
    state: QueryState, driver: AsyncDriver,
) -> Optional[dict]:
    tmatch = match_template(state["query"])
    if tmatch is None:
        return None
    template = _TEMPLATE_CATALOG.get(tmatch.template_name)
    if template is None:
        return None
    acl_cypher, acl_params = _apply_acl(template.cypher, state)
    tenant_id = state.get("tenant_id", "")
    merged_params = {**tmatch.params, **acl_params, "tenant_id": tenant_id}
    records = await _execute_sandboxed_read(
        driver, acl_cypher, merged_params,
    )
    if records is None or not records:
        return None
    return {
        "cypher_query": template.cypher,
        "cypher_results": records,
        "iteration_count": 0,
    }


async def cypher_retrieve(state: QueryState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.cypher_retrieve"):
        start = time.monotonic()
        try:
            async with _neo4j_session() as driver:
                template_result = await _try_template_match(state, driver)
                if template_result is not None:
                    return template_result

                if not dynamic_cypher_allowed():
                    _query_logger.info(
                        "Dynamic Cypher generation disabled (ALLOW_DYNAMIC_CYPHER=false). "
                        "No template matched for query."
                    )
                    return {
                        "cypher_query": "",
                        "cypher_results": [],
                        "iteration_count": 0,
                    }

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
                    records = await _execute_sandboxed_read(
                        driver, acl_cypher, acl_params,
                    )
                    if records is None:
                        return {
                            "cypher_query": validated,
                            "cypher_results": [],
                            "iteration_count": iteration,
                        }
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
        return await _fetch_candidates(driver, state)


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
        agg_records = await _execute_sandboxed_read(
            driver, agg_acl_cypher, agg_acl_params,
        )
        if agg_records is None:
            return {
                "candidates": candidates,
                "cypher_query": validated_agg,
                "cypher_results": [],
                "iteration_count": 1,
            }

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


_DEFAULT_RERANKER = BM25Reranker()
_DEFAULT_TOKEN_BUDGET = TokenBudget()


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

    reranked = _DEFAULT_RERANKER.rerank(state["query"], context)
    ranked_context = [sc.data for sc in reranked]
    truncated = truncate_context(ranked_context, _DEFAULT_TOKEN_BUDGET)

    answer = await _llm_synthesize(state["query"], truncated)
    return {"answer": answer, "sources": truncated}


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
            _query_logger.warning(
                "RAG evaluation failed, continuing without score", exc_info=True,
            )
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
