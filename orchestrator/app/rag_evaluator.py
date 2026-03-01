from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from pydantic import BaseModel, Field, ValidationError

from orchestrator.app.redis_client import create_async_redis, require_redis
from orchestrator.app.vector_store import _cosine_similarity

logger = logging.getLogger(__name__)

_JSON_OBJECT_PATTERN = re.compile(r"\{.*\}", re.DOTALL)


def _extract_json(raw: str) -> Optional[dict]:
    match = _JSON_OBJECT_PATTERN.search(raw)
    if match is None:
        return None
    try:
        return json.loads(match.group())
    except (json.JSONDecodeError, ValueError):
        return None


class JudgeOutput(BaseModel):
    faithfulness: float = Field(ge=0.0, le=1.0)
    groundedness: float = Field(ge=0.0, le=1.0)
    reasoning: Optional[str] = None


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
    contradiction_count: int = 0

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
    embed_fn: Optional[Callable[[str], List[float]]] = None,
    similarity_threshold: float = 0.8,
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

    if embed_fn is not None:
        context_embeddings = {entity: embed_fn(entity) for entity in context_entities}
        ungrounded = []
        for entity in meaningful_entities:
            entity_embedding = embed_fn(entity)
            max_sim = max(
                (
                    _cosine_similarity(entity_embedding, ctx_emb)
                    for ctx_emb in context_embeddings.values()
                ),
                default=0.0,
            )
            if max_sim < similarity_threshold:
                ungrounded.append(entity)
    else:
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

    answer_lower = {e.lower() for e in answer_entities if len(e) > 2}
    context_lower = {e.lower() for e in context_entities}

    if not answer_lower:
        return 1.0

    def _is_grounded(entity: str) -> bool:
        if entity in context_lower:
            return True
        return any(entity in ctx or ctx in entity for ctx in context_lower)

    verified = sum(1 for e in answer_lower if _is_grounded(e))
    return verified / len(answer_lower)


def _compute_semantic_groundedness(
    answer: str,
    sources: List[Dict[str, Any]],
) -> float:
    if not answer or not sources:
        return 1.0

    answer_entities = _extract_entity_names_from_text(answer)
    context_entities = _extract_entity_names_from_context(sources)

    context_text = " ".join(
        str(v) for s in sources for v in s.values()
        if isinstance(v, (str, list))
    )
    context_lower = context_text.lower()

    answer_lower = {e.lower() for e in answer_entities if len(e) > 2}
    if not answer_lower:
        return 1.0

    context_entity_lower = {e.lower() for e in context_entities}

    def _semantic_match(entity: str) -> bool:
        if entity in context_entity_lower:
            return True
        if any(entity in ctx or ctx in entity for ctx in context_entity_lower):
            return True
        if entity in context_lower:
            return True
        tokens = entity.split()
        if len(tokens) > 1:
            return sum(1 for t in tokens if t in context_lower) >= len(tokens) // 2
        return False

    verified = sum(1 for e in answer_lower if _semantic_match(e))
    return verified / len(answer_lower)


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


CONTRADICTION_JUDGE_PROMPT = (
    "You are a strict graph topology contradiction detector.\n"
    "Given an ANSWER and SOURCE CONTEXT from a knowledge graph, identify:\n"
    "1. Direction reversals: Does the answer reverse the direction of a relationship?\n"
    "   Example: sources say 'A calls B' but answer says 'B calls A'.\n"
    "2. Negation contradictions: Does the answer negate a claim present in sources?\n"
    "   Example: sources say 'A calls B' but answer says 'A does NOT call B'.\n"
    "3. Fabricated relationships: Does the answer claim relationships not in sources?\n\n"
    "ANSWER: {answer}\n\n"
    "SOURCES:\n{sources}\n\n"
    "Respond ONLY with a JSON object:\n"
    "{{\"contradiction_count\": <int>, \"contradictions\": [<string descriptions>]}}"
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
            extracted = _extract_json(raw_response)
            if extracted is None:
                raise ValueError("No JSON object found in LLM response")
            validated = JudgeOutput(**extracted)
            faithfulness = validated.faithfulness
            groundedness = validated.groundedness
        except (json.JSONDecodeError, ValueError, TypeError, KeyError, ValidationError):
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


_INVALID_QUALITIES = frozenset({"error", "skipped", "pending", "no_embedding"})


def is_valid_evaluation(entry: Dict[str, Any]) -> bool:
    quality = entry.get("retrieval_quality", "")
    if quality in _INVALID_QUALITIES:
        return False
    score = entry.get("evaluation_score")
    return score is not None and isinstance(score, (int, float))


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

    def get_valid_scores(self) -> List[Dict[str, Any]]:
        self._evict_expired()
        return [v for v in self._data.values() if is_valid_evaluation(v)]

    def average_score(self) -> Optional[float]:
        valid = self.get_valid_scores()
        if not valid:
            return None
        total = sum(v["evaluation_score"] for v in valid)
        return total / len(valid)

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
        require_redis("RedisEvaluationStore")
        self._redis = create_async_redis(redis_url, password=password, db=db)
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
        except Exception:
            logger.debug("Redis eval-store get failed, falling back to local miss")
        return None

    async def put(self, query_id: str, result: Dict[str, Any]) -> None:
        self._local.put(query_id, result)
        try:
            raw = json.dumps(result, default=str)
            await self._redis.setex(self._rkey(query_id), self._ttl, raw)
        except Exception:
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
