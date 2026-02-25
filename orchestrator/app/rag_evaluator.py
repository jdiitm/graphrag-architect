from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

try:
    import redis.asyncio as aioredis
except ImportError:  # pragma: no cover
    aioredis = None  # type: ignore[assignment]

from orchestrator.app.vector_store import _cosine_similarity

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RelevanceScore:
    query: str
    score: float
    context_count: int
    retrieval_path: str


@dataclass(frozen=True)
class EvaluationResult:
    context_relevance: float
    faithfulness: float
    groundedness: float
    ungrounded_claims: List[str] = field(default_factory=list)
    context_count: int = 0
    retrieval_path: str = "vector"
    used_fallback: bool = False

    @property
    def score(self) -> float:
        return (
            0.3 * self.context_relevance
            + 0.4 * self.faithfulness
            + 0.3 * self.groundedness
        )


def evaluate_relevance(
    query_embedding: List[float],
    context_embeddings: List[List[float]],
) -> float:
    if not context_embeddings:
        return 0.0
    total = sum(
        _cosine_similarity(query_embedding, ctx)
        for ctx in context_embeddings
    )
    return total / len(context_embeddings)


_ENTITY_PATTERN = re.compile(
    r"\b([A-Za-z][\w-]*(?:-[A-Za-z][\w-]*)*)(?:\s+(?:service|topic|database|deployment))?",
)


def _extract_entity_names_from_text(text: str) -> Set[str]:
    return {m.group(1).lower() for m in _ENTITY_PATTERN.finditer(text)}


def _extract_entity_names_from_context(
    sources: List[Dict[str, Any]],
) -> Set[str]:
    names: Set[str] = set()
    for source in sources:
        for key in ("name", "id", "source", "target",
                    "affected_service", "consumer_service",
                    "producer_service", "service"):
            val = source.get(key)
            if isinstance(val, str) and val:
                names.add(val.lower())
        result = source.get("result")
        if isinstance(result, dict):
            for key in ("name", "id"):
                val = result.get(key)
                if isinstance(val, str) and val:
                    names.add(val.lower())
    return names


def _compute_faithfulness(
    answer: str,
    sources: List[Dict[str, Any]],
) -> tuple:
    if not answer or not sources:
        return 1.0, []

    answer_entities = _extract_entity_names_from_text(answer)
    context_entities = _extract_entity_names_from_context(sources)

    stop_words = {
        "the", "a", "an", "is", "are", "was", "were", "be", "been",
        "being", "have", "has", "had", "do", "does", "did", "will",
        "would", "could", "should", "may", "might", "shall", "can",
        "not", "no", "and", "or", "but", "if", "then", "else",
        "when", "where", "how", "what", "which", "who", "whom",
        "this", "that", "these", "those", "it", "its", "to", "of",
        "in", "for", "on", "with", "at", "by", "from", "as", "into",
        "through", "during", "before", "after", "above", "below",
        "between", "out", "off", "over", "under", "again", "further",
        "all", "each", "every", "both", "few", "more", "most",
        "other", "some", "such", "only", "very",
    }

    meaningful_entities = {e for e in answer_entities if e not in stop_words and len(e) > 2}
    if not meaningful_entities:
        return 1.0, []

    ungrounded = [e for e in meaningful_entities if e not in context_entities]
    if not meaningful_entities:
        return 1.0, ungrounded

    coverage = 1.0 - len(ungrounded) / len(meaningful_entities)
    return max(0.0, coverage), ungrounded


def _compute_groundedness(
    answer: str,
    sources: List[Dict[str, Any]],
) -> float:
    if not answer or not sources:
        return 1.0

    answer_entities = _extract_entity_names_from_text(answer)
    context_entities = _extract_entity_names_from_context(sources)

    stop_words = {
        "the", "a", "an", "is", "are", "was", "were", "be", "been",
        "being", "have", "has", "had", "do", "does", "did", "will",
        "would", "could", "should", "may", "might", "shall", "can",
        "not", "no", "and", "or", "but", "if", "then", "else",
        "when", "where", "how", "what", "which", "who", "whom",
        "this", "that", "these", "those", "it", "its", "to", "of",
        "in", "for", "on", "with", "at", "by", "from", "as", "into",
        "through", "during", "before", "after", "above", "below",
        "between", "out", "off", "over", "under", "again", "further",
        "all", "each", "every", "both", "few", "more", "most",
        "other", "some", "such", "only", "very",
    }

    meaningful = {e for e in answer_entities if e not in stop_words and len(e) > 2}
    if not meaningful:
        return 1.0

    verified = sum(1 for e in meaningful if e in context_entities)
    return verified / len(meaningful)


class RAGEvaluator:
    def __init__(self, low_relevance_threshold: float = 0.3) -> None:
        self._threshold = low_relevance_threshold

    def evaluate(
        self,
        query: str,
        query_embedding: List[float],
        context_embeddings: List[List[float]],
        retrieval_path: str = "vector",
    ) -> RelevanceScore:
        score = evaluate_relevance(query_embedding, context_embeddings)
        return RelevanceScore(
            query=query,
            score=score,
            context_count=len(context_embeddings),
            retrieval_path=retrieval_path,
        )

    def evaluate_faithfulness(
        self,
        query: str,
        answer: str,
        sources: List[Dict[str, Any]],
        query_embedding: List[float],
        context_embeddings: List[List[float]],
        retrieval_path: str = "vector",
    ) -> EvaluationResult:
        context_relevance = evaluate_relevance(
            query_embedding, context_embeddings,
        )
        faithfulness_score, ungrounded = _compute_faithfulness(answer, sources)
        groundedness = _compute_groundedness(answer, sources)

        return EvaluationResult(
            context_relevance=context_relevance,
            faithfulness=faithfulness_score,
            groundedness=groundedness,
            ungrounded_claims=ungrounded,
            context_count=len(context_embeddings),
            retrieval_path=retrieval_path,
        )

    def is_low_relevance(self, score: RelevanceScore) -> bool:
        return score.score < self._threshold


_JUDGE_PROMPT_TEMPLATE = (
    "You are an expert evaluator for a Retrieval-Augmented Generation system.\n"
    "Given a QUERY, an ANSWER, and SOURCE CONTEXT, evaluate:\n"
    "1. faithfulness: Does the answer only contain claims supported by sources? (0.0-1.0)\n"
    "2. groundedness: What fraction of entities in the answer appear in sources? (0.0-1.0)\n\n"
    "QUERY: {query}\n\n"
    "ANSWER: {answer}\n\n"
    "SOURCES:\n{sources}\n\n"
    "Respond ONLY with a JSON object: {{\"faithfulness\": <float>, \"groundedness\": <float>}}"
)


class LLMEvaluator:
    def __init__(
        self,
        judge_fn: Callable[[str], Awaitable[str]],
    ) -> None:
        self._judge_fn = judge_fn

    async def evaluate(
        self,
        query: str,
        answer: str,
        sources: List[Dict[str, Any]],
        query_embedding: Optional[List[float]] = None,
        context_embeddings: Optional[List[List[float]]] = None,
    ) -> EvaluationResult:
        sources_text = json.dumps(sources, indent=2, default=str)[:4000]
        prompt = _JUDGE_PROMPT_TEMPLATE.format(
            query=query, answer=answer, sources=sources_text,
        )
        used_fallback = False
        try:
            raw_response = await self._judge_fn(prompt)
            scores = json.loads(raw_response)
            faithfulness = float(scores.get("faithfulness", 0.5))
            groundedness = float(scores.get("groundedness", 0.5))
        except (json.JSONDecodeError, ValueError, TypeError, KeyError):
            logger.warning("LLM judge returned unparseable response, using lexical fallback")
            used_fallback = True
            faithfulness_score, _ = _compute_faithfulness(answer, sources)
            groundedness = _compute_groundedness(answer, sources)
            faithfulness = faithfulness_score

        faithfulness = max(0.0, min(1.0, faithfulness))
        groundedness = max(0.0, min(1.0, groundedness))

        context_relevance = 0.0
        if query_embedding is not None and context_embeddings is not None:
            context_relevance = evaluate_relevance(query_embedding, context_embeddings)

        return EvaluationResult(
            context_relevance=context_relevance,
            faithfulness=faithfulness,
            groundedness=groundedness,
            context_count=len(sources),
            used_fallback=used_fallback,
        )


class EvaluationStore:
    def __init__(self, ttl_seconds: float = 600.0) -> None:
        self._data: Dict[str, Dict[str, Any]] = {}
        self._timestamps: Dict[str, float] = {}
        self._ttl = ttl_seconds

    def put(self, query_id: str, result: Dict[str, Any]) -> None:
        self._evict_expired()
        self._data[query_id] = result
        self._timestamps[query_id] = time.monotonic()

    def get(self, query_id: str) -> Optional[Dict[str, Any]]:
        self._evict_expired()
        return self._data.get(query_id)

    def _evict_expired(self) -> None:
        now = time.monotonic()
        expired = [
            qid for qid, ts in self._timestamps.items()
            if (now - ts) > self._ttl
        ]
        for qid in expired:
            self._data.pop(qid, None)
            self._timestamps.pop(qid, None)


class RedisEvaluationStore:
    def __init__(
        self,
        redis_url: str,
        ttl_seconds: int = 600,
        key_prefix: str = "graphrag:evalstore:",
        password: str = "",
        db: int = 0,
    ) -> None:
        if aioredis is None:
            raise ImportError("redis package is required for RedisEvaluationStore")
        kwargs: dict[str, Any] = {"decode_responses": True, "db": db}
        if password:
            kwargs["password"] = password
        self._redis = aioredis.from_url(redis_url, **kwargs)
        self._ttl = ttl_seconds
        self._prefix = key_prefix
        self._local = EvaluationStore(ttl_seconds=float(ttl_seconds))

    def _rkey(self, query_id: str) -> str:
        return f"{self._prefix}{query_id}"

    async def get(self, query_id: str) -> Optional[Dict[str, Any]]:
        local_result = self._local.get(query_id)
        if local_result is not None:
            return local_result
        try:
            raw = await self._redis.get(self._rkey(query_id))
            if raw is not None:
                value = json.loads(raw)
                self._local.put(query_id, value)
                return value
        except Exception:  # pylint: disable=broad-except
            logger.debug("Redis eval-store get failed, falling back to local miss")
        return None

    async def put(self, query_id: str, result: Dict[str, Any]) -> None:
        self._local.put(query_id, result)
        try:
            raw = json.dumps(result, default=str)
            await self._redis.setex(self._rkey(query_id), self._ttl, raw)
        except Exception:  # pylint: disable=broad-except
            logger.debug("Redis eval-store put failed, local still updated")


def create_evaluation_store() -> Any:
    from orchestrator.app.config import RedisConfig
    redis_cfg = RedisConfig.from_env()
    if redis_cfg.url:
        return RedisEvaluationStore(
            redis_url=redis_cfg.url,
            password=redis_cfg.password,
            db=redis_cfg.db,
        )
    return EvaluationStore()
