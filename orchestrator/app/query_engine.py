import asyncio
import inspect
import logging
import os
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
from orchestrator.app.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
)
from orchestrator.app.config import AuthConfig, EmbeddingConfig, ExtractionConfig, RAGEvalConfig
from orchestrator.app.cypher_sandbox import (
    SandboxedQueryExecutor,
    TemplateHashRegistry,
)
from orchestrator.app.cypher_validator import CypherValidationError, estimate_query_cost
from orchestrator.app.context_manager import (
    TokenBudget,
    format_context_for_prompt,
    truncate_context_topology,
)
from orchestrator.app.prompt_sanitizer import sanitize_query_input
from orchestrator.app.graph_embeddings import compute_centroid, rerank_with_structural
from orchestrator.app.reranker import BM25Reranker
from orchestrator.app.agentic_traversal import run_traversal
from orchestrator.app.query_templates import (
    TemplateCatalog,
    match_template,
)
from orchestrator.app.subgraph_cache import (
    create_subgraph_cache,
    normalize_cypher,
)
from orchestrator.app.config import VectorStoreConfig
from orchestrator.app.vector_store import SearchResult, create_vector_store
from orchestrator.app.neo4j_pool import get_driver, get_query_timeout
from orchestrator.app.observability import QUERY_DURATION, get_tracer
from orchestrator.app.query_classifier import classify_query
from orchestrator.app.query_models import QueryComplexity, QueryState
from orchestrator.app.rag_evaluator import EvaluationStore, LLMEvaluator, RAGEvaluator
from orchestrator.app.semantic_cache import SemanticQueryCache

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

_TEMPLATE_CATALOG = TemplateCatalog()
_TEMPLATE_REGISTRY = TemplateHashRegistry(_TEMPLATE_CATALOG)
_SANDBOX = SandboxedQueryExecutor(registry=_TEMPLATE_REGISTRY)
_SUBGRAPH_CACHE = create_subgraph_cache()
_SEMANTIC_CACHE = SemanticQueryCache()

_VS_CFG = VectorStoreConfig.from_env()
_VECTOR_STORE = create_vector_store(
    backend=_VS_CFG.backend, url=_VS_CFG.qdrant_url, api_key=_VS_CFG.qdrant_api_key,
    pool_size=_VS_CFG.pool_size,
)

_VECTOR_COLLECTION = "service_embeddings"

_CB_LLM = CircuitBreaker(
    CircuitBreakerConfig(failure_threshold=3, recovery_timeout=30.0),
    name="llm-synthesize",
)
_CB_EMBEDDING = CircuitBreaker(
    CircuitBreakerConfig(failure_threshold=5, recovery_timeout=20.0),
    name="embedding",
)


_DEFAULT_MAX_QUERY_COST = 20


def _get_max_query_cost() -> int:
    raw = os.environ.get("MAX_QUERY_COST", str(_DEFAULT_MAX_QUERY_COST))
    try:
        return int(raw)
    except ValueError:
        return _DEFAULT_MAX_QUERY_COST


def _sandbox_inject_limit(cypher: str) -> str:
    return _SANDBOX.inject_limit(cypher)



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


async def _raw_embed_query(text: str) -> Optional[List[float]]:
    cfg, client = _get_embedding_resources()
    if cfg is not None and cfg.provider == "openai" and client is not None:
        response = await client.embeddings.create(
            input=[text], model=cfg.model_name
        )
        return response.data[0].embedding
    return None


async def _embed_query(text: str) -> Optional[List[float]]:
    try:
        return await _CB_EMBEDDING.call(_raw_embed_query, text)
    except CircuitOpenError:
        _query_logger.warning("Embedding circuit open, falling back to fulltext")
        return None
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


def _build_traversal_acl_params(state: QueryState) -> Dict[str, Any]:
    auth_config = AuthConfig.from_env()
    if auth_config.require_tokens and not auth_config.token_secret:
        raise AuthConfigurationError(
            "server misconfigured: token verification required but no secret set"
        )
    principal = SecurityPrincipal.from_header(
        state.get("authorization", ""),
        token_secret=auth_config.token_secret,
    )
    namespaces = [principal.namespace] if principal.namespace != "*" else []
    return {
        "is_admin": principal.is_admin,
        "acl_team": principal.team,
        "acl_namespaces": namespaces,
    }


def _build_llm() -> ChatGoogleGenerativeAI:
    config = ExtractionConfig.from_env()
    return ChatGoogleGenerativeAI(
        model=config.model_name,
        google_api_key=config.google_api_key,
    )


def _build_llm_judge_fn() -> Optional[Any]:
    try:
        llm = _build_llm()
    except (KeyError, Exception):
        return None

    async def _judge(prompt: str) -> str:
        response = await llm.ainvoke(prompt)
        return response.content.strip()

    return _judge


async def _raw_llm_synthesize(
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


async def _llm_synthesize(
    query: str,
    context: List[Dict[str, Any]],
) -> str:
    try:
        return await _CB_LLM.call(_raw_llm_synthesize, query, context)
    except CircuitOpenError:
        _query_logger.warning("LLM circuit open, returning degraded response")
        return (
            "The LLM synthesis service is temporarily unavailable due to "
            "circuit breaker activation. Please retry shortly."
        )


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
    if tenant_id:
        base_cypher = (
            "CALL db.index.fulltext.queryNodes('service_name_index', $query) "
            "YIELD node, score "
            "WHERE node.tenant_id = $tenant_id "
            "RETURN node {.*, score: score} AS result "
            "ORDER BY score DESC LIMIT $limit"
        )
    vec_cypher, vec_acl = _apply_acl(base_cypher, state, alias="node")

    async def _vector_tx(tx: AsyncManagedTransaction) -> list:
        params: Dict[str, Any] = {
            **vec_acl, "limit": limit, "query": state["query"],
        }
        if tenant_id:
            params["tenant_id"] = tenant_id
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


async def _cache_get(key: str) -> Optional[list]:
    result = _SUBGRAPH_CACHE.get(key)
    if inspect.isawaitable(result):
        return await result
    return result


async def _cache_put(key: str, value: list) -> None:
    result = _SUBGRAPH_CACHE.put(key, value)
    if inspect.isawaitable(result):
        await result


async def _execute_sandboxed_read(
    driver: AsyncDriver,
    cypher: str,
    acl_params: Dict[str, str],
) -> Optional[list]:
    cost = estimate_query_cost(cypher)
    max_cost = _get_max_query_cost()
    if cost > max_cost:
        raise CypherValidationError(
            f"Query cost {cost} exceeds maximum allowed cost {max_cost}"
        )

    cache_key = normalize_cypher(cypher) + "|" + str(sorted(acl_params.items()))
    cached = await _cache_get(cache_key)
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
        await _cache_put(cache_key, records)
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
    _SANDBOX.validate(template.cypher)
    tenant_id = state.get("tenant_id", "")
    acl_params = _build_traversal_acl_params(state)
    merged_params = {
        **tmatch.params,
        **acl_params,
        "tenant_id": tenant_id,
    }
    records = await _execute_sandboxed_read(
        driver, template.cypher, merged_params,
    )
    if records is None or not records:
        return None
    return {
        "cypher_query": template.cypher,
        "cypher_results": records,
        "iteration_count": 0,
    }


async def _check_semantic_cache(
    query: str, tenant_id: str,
) -> Tuple[Optional[dict], Optional[List[float]], bool]:
    if _SEMANTIC_CACHE is None:
        return None, None, False
    query_embedding = await _embed_query(query)
    if query_embedding is None:
        return None, None, False
    result, is_owner = await _SEMANTIC_CACHE.lookup_or_wait(
        query_embedding, tenant_id=tenant_id,
    )
    return result, query_embedding, is_owner


def _store_in_semantic_cache(
    query: str,
    query_embedding: Optional[list],
    result: dict,
    tenant_id: str,
    complexity: str = "",
) -> None:
    if _SEMANTIC_CACHE is None or query_embedding is None:
        return
    _SEMANTIC_CACHE.store(
        query=query,
        query_embedding=query_embedding,
        result=result,
        tenant_id=tenant_id,
        complexity=complexity,
    )
    _SEMANTIC_CACHE.notify_complete(query_embedding)


async def cypher_retrieve(state: QueryState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.cypher_retrieve"):
        start = time.monotonic()
        try:
            tenant_id = state.get("tenant_id", "")
            cached, query_embedding, is_owner = await _check_semantic_cache(
                state["query"], tenant_id,
            )
            if cached is not None:
                _query_logger.debug("Semantic cache hit for cypher_retrieve")
                return cached

            try:
                async with _neo4j_session() as driver:
                    template_result = await _try_template_match(state, driver)
                    if template_result is not None:
                        _store_in_semantic_cache(
                            state["query"], query_embedding,
                            template_result, tenant_id,
                        )
                        return template_result

                    candidates = await _fetch_candidates(driver, state)
                    if not candidates:
                        if is_owner and query_embedding is not None:
                            _SEMANTIC_CACHE.notify_complete(
                                query_embedding, failed=True,
                            )
                        return {
                            "cypher_query": "",
                            "cypher_results": [],
                            "iteration_count": 0,
                        }

                    start_node_id = _extract_start_node(candidates)
                    acl_params = _build_traversal_acl_params(state)

                    context = await run_traversal(
                        driver=driver,
                        start_node_id=start_node_id,
                        tenant_id=tenant_id,
                        acl_params=acl_params,
                        timeout=_get_query_timeout(),
                    )

                    result = {
                        "cypher_query": "agentic_traversal",
                        "cypher_results": context,
                        "iteration_count": 0,
                    }
                    _store_in_semantic_cache(
                        state["query"], query_embedding,
                        result, tenant_id,
                        complexity=str(state.get("complexity", "")),
                    )
                    return result
            except Exception:
                if is_owner and query_embedding is not None:
                    _SEMANTIC_CACHE.notify_complete(
                        query_embedding, failed=True,
                    )
                raise
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "cypher_retrieve"}
            )


def _extract_start_node(candidates: List[Dict[str, Any]]) -> str:
    for candidate in candidates:
        node_id = candidate.get("id") or candidate.get("name", "")
        if node_id:
            return str(node_id)
    return ""


async def hybrid_retrieve(state: QueryState) -> dict:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.hybrid_retrieve"):
        start = time.monotonic()
        try:
            async with _neo4j_session() as driver:
                candidates = await _fetch_candidates(driver, state)

                tmatch = match_template(state["query"])
                if tmatch is not None:
                    template = _TEMPLATE_CATALOG.get(tmatch.template_name)
                    if template is not None:
                        _SANDBOX.validate(template.cypher)
                        tenant_id = state.get("tenant_id", "")
                        acl_params = _build_traversal_acl_params(state)
                        merged = {
                            **tmatch.params,
                            **acl_params,
                            "tenant_id": tenant_id,
                        }
                        agg_records = await _execute_sandboxed_read(
                            driver, template.cypher, merged,
                        )
                        return {
                            "candidates": candidates,
                            "cypher_query": template.cypher,
                            "cypher_results": agg_records or [],
                            "iteration_count": 0,
                        }

                return {
                    "candidates": candidates,
                    "cypher_query": "",
                    "cypher_results": [],
                    "iteration_count": 0,
                }
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "hybrid_retrieve"}
            )


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
_STRUCTURAL_EMBEDDINGS: Dict[str, List[float]] = {}


def set_structural_embeddings(embeddings: Dict[str, List[float]]) -> None:
    _STRUCTURAL_EMBEDDINGS.clear()
    _STRUCTURAL_EMBEDDINGS.update(embeddings)


def _apply_structural_rerank(
    candidates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not _STRUCTURAL_EMBEDDINGS or not candidates:
        return candidates

    search_results = [
        SearchResult(
            id=str(c.get("id", c.get("name", ""))),
            score=float(c.get("score", 0.0)),
            metadata=c,
        )
        for c in candidates
    ]

    available = [
        _STRUCTURAL_EMBEDDINGS[r.id]
        for r in search_results
        if r.id in _STRUCTURAL_EMBEDDINGS
    ]
    if not available:
        return candidates

    query_structural = compute_centroid(available)

    reranked = rerank_with_structural(
        search_results, _STRUCTURAL_EMBEDDINGS, query_structural,
    )
    return [r.metadata for r in reranked]


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
    ranked_context = _apply_structural_rerank(ranked_context)
    truncated = truncate_context_topology(ranked_context, _DEFAULT_TOKEN_BUDGET)

    answer = await _llm_synthesize(state["query"], truncated)
    return {"answer": answer, "sources": truncated}


_EVAL_STORE = EvaluationStore()


def get_eval_store() -> EvaluationStore:
    return _EVAL_STORE


def _collect_context_embeddings(state: dict) -> List[List[float]]:
    embeddings: List[List[float]] = []
    for source in state.get("sources", []):
        if isinstance(source, dict):
            emb = source.get("embedding")
            if isinstance(emb, list):
                embeddings.append(emb)
    return embeddings


def _classify_quality(score: float, threshold: float) -> str:
    if score >= 0.7:
        return "high"
    if score < threshold:
        return "low"
    return "adequate"


async def _select_and_run_evaluator(
    eval_config: RAGEvalConfig,
    state: dict,
    query_embedding: List[float],
    context_embeddings: List[List[float]],
) -> Tuple[float, str]:
    judge_fn = _build_llm_judge_fn() if eval_config.use_llm_judge else None

    if judge_fn is not None:
        llm_result = await LLMEvaluator(judge_fn=judge_fn).evaluate(
            query=state["query"],
            answer=state.get("answer", ""),
            sources=state.get("sources", []),
            query_embedding=query_embedding,
            context_embeddings=context_embeddings or None,
        )
        quality = _classify_quality(llm_result.score, eval_config.low_relevance_threshold)
        _query_logger.info(
            "RAG evaluation (LLM judge): score=%.3f quality=%s contexts=%d",
            llm_result.score, quality, llm_result.context_count,
        )
        return llm_result.score, quality

    evaluator = RAGEvaluator(low_relevance_threshold=eval_config.low_relevance_threshold)
    result = evaluator.evaluate(
        query=state["query"],
        query_embedding=query_embedding,
        context_embeddings=context_embeddings,
        retrieval_path=state.get("retrieval_path", "vector"),
    )
    quality = _classify_quality(result.score, eval_config.low_relevance_threshold)
    _query_logger.info(
        "RAG evaluation: score=%.3f quality=%s path=%s contexts=%d",
        result.score, quality, result.retrieval_path, result.context_count,
    )
    return result.score, quality


async def _run_background_evaluation(
    query_id: str,
    state: dict,
) -> None:
    tracer = get_tracer()
    with tracer.start_as_current_span("query.evaluate_background"):
        start = time.monotonic()
        try:
            eval_config = RAGEvalConfig.from_env()
            query_embedding = await _embed_query(state["query"])
            if query_embedding is None:
                _EVAL_STORE.put(query_id, {
                    "evaluation_score": None,
                    "retrieval_quality": "no_embedding",
                    "query_id": query_id,
                })
                return

            context_embeddings = _collect_context_embeddings(state)
            score, quality = await _select_and_run_evaluator(
                eval_config, state, query_embedding, context_embeddings,
            )
            _EVAL_STORE.put(query_id, {
                "evaluation_score": score,
                "retrieval_quality": quality,
                "query_id": query_id,
            })
        except Exception:
            _query_logger.warning(
                "RAG evaluation failed, continuing without score", exc_info=True,
            )
            _EVAL_STORE.put(query_id, {
                "evaluation_score": None,
                "retrieval_quality": "error",
                "query_id": query_id,
            })
        finally:
            QUERY_DURATION.record(
                (time.monotonic() - start) * 1000, {"node": "evaluate"}
            )


def _on_eval_task_done(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        _query_logger.error("Background evaluation failed: %s", exc, exc_info=exc)


async def evaluate_response(state: QueryState) -> dict:
    eval_config = RAGEvalConfig.from_env()
    if not eval_config.enable_evaluation:
        return {"evaluation_score": None, "retrieval_quality": "skipped", "query_id": ""}

    query_id = f"eval-{id(state)}-{time.monotonic_ns()}"
    task = asyncio.create_task(
        _run_background_evaluation(query_id, dict(state)),
    )
    task.add_done_callback(_on_eval_task_done)
    return {"evaluation_score": None, "retrieval_quality": "pending", "query_id": query_id}


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
