from __future__ import annotations

import os
from dataclasses import dataclass


_KNOWN_MODES = {"dev", "production"}


@dataclass(frozen=True)
class AuthConfig:
    token_secret: str = ""
    token_ttl_seconds: int = 3600
    require_tokens: bool = False
    deployment_mode: str = "dev"
    default_deny_untagged: bool = True

    @classmethod
    def from_env(cls) -> AuthConfig:
        mode = os.environ.get("DEPLOYMENT_MODE", "dev").lower()

        if mode not in _KNOWN_MODES:
            raise SystemExit(
                f"FATAL: DEPLOYMENT_MODE={mode!r} is not recognized. "
                f"Valid modes: {', '.join(sorted(_KNOWN_MODES))}"
            )

        secret = os.environ.get("AUTH_TOKEN_SECRET", "")
        raw_require = os.environ.get("AUTH_REQUIRE_TOKENS", "").lower()
        if raw_require == "false":
            require_tokens = False
        else:
            require_tokens = True

        if mode == "production" and not secret:
            raise SystemExit(
                "FATAL: DEPLOYMENT_MODE=production but AUTH_TOKEN_SECRET is not set. "
                "Refusing to start without authentication credentials."
            )

        deny_untagged = os.environ.get(
            "ACL_DEFAULT_DENY_UNTAGGED", "true" if mode == "production" else "false"
        ).lower() == "true"

        return cls(
            token_secret=secret,
            token_ttl_seconds=int(os.environ.get("AUTH_TOKEN_TTL", "3600")),
            require_tokens=require_tokens,
            deployment_mode=mode,
            default_deny_untagged=deny_untagged,
        )


@dataclass(frozen=True)
class Neo4jConfig:
    uri: str
    username: str
    password: str
    query_timeout: float = 30.0
    database: str = "neo4j"

    @classmethod
    def from_env(cls) -> Neo4jConfig:
        return cls(
            uri=os.environ.get("NEO4J_URI", "neo4j://localhost:7687"),
            username=os.environ.get("NEO4J_USERNAME", "neo4j"),
            password=os.environ["NEO4J_PASSWORD"],
            query_timeout=float(os.environ.get("NEO4J_QUERY_TIMEOUT", "30")),
            database=os.environ.get("NEO4J_DATABASE", "neo4j"),
        )


@dataclass(frozen=True)
class ExtractionConfig:
    google_api_key: str
    model_name: str = "gemini-2.0-flash"
    max_concurrency: int = 5
    token_budget_per_batch: int = 8_000
    max_retries: int = 5
    retry_min_wait: float = 1.0
    retry_max_wait: float = 60.0
    llm_provider: str = "gemini"

    @classmethod
    def from_env(cls) -> ExtractionConfig:
        return cls(
            google_api_key=os.environ["GOOGLE_API_KEY"],
            model_name=os.environ.get("EXTRACTION_MODEL", "gemini-2.0-flash"),
            max_concurrency=int(os.environ.get("EXTRACTION_MAX_CONCURRENCY", "5")),
            token_budget_per_batch=int(
                os.environ.get("EXTRACTION_TOKEN_BUDGET", "200000")
            ),
            max_retries=int(os.environ.get("EXTRACTION_MAX_RETRIES", "5")),
            retry_min_wait=float(os.environ.get("EXTRACTION_RETRY_MIN_WAIT", "1.0")),
            retry_max_wait=float(os.environ.get("EXTRACTION_RETRY_MAX_WAIT", "60.0")),
            llm_provider=os.environ.get("LLM_PROVIDER", "gemini"),
        )


@dataclass(frozen=True)
class KafkaConsumerConfig:
    brokers: str = "localhost:9092"
    topic: str = "raw-documents"
    group_id: str = "orchestrator-consumers"

    @classmethod
    def from_env(cls) -> KafkaConsumerConfig:
        return cls(
            brokers=os.environ.get("KAFKA_BROKERS", "localhost:9092"),
            topic=os.environ.get("KAFKA_TOPIC", "raw-documents"),
            group_id=os.environ.get("KAFKA_CONSUMER_GROUP", "orchestrator-consumers"),
        )


@dataclass(frozen=True)
class RateLimitConfig:
    max_concurrent_ingestions: int = 10

    @classmethod
    def from_env(cls) -> RateLimitConfig:
        return cls(
            max_concurrent_ingestions=int(
                os.environ.get("MAX_CONCURRENT_INGESTIONS", "10")
            ),
        )


@dataclass(frozen=True)
class VectorStoreConfig:
    backend: str = "memory"
    qdrant_url: str = ""
    qdrant_api_key: str = ""

    @classmethod
    def from_env(cls) -> VectorStoreConfig:
        return cls(
            backend=os.environ.get("VECTOR_STORE_BACKEND", "memory"),
            qdrant_url=os.environ.get("QDRANT_URL", ""),
            qdrant_api_key=os.environ.get("QDRANT_API_KEY", ""),
        )


@dataclass(frozen=True)
class EmbeddingConfig:
    model_name: str = "text-embedding-3-small"
    dimensions: int = 1536
    batch_size: int = 100
    provider: str = "openai"

    @classmethod
    def from_env(cls) -> EmbeddingConfig:
        return cls(
            model_name=os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small"),
            dimensions=int(os.environ.get("EMBEDDING_DIMENSIONS", "1536")),
            batch_size=int(os.environ.get("EMBEDDING_BATCH_SIZE", "100")),
            provider=os.environ.get("EMBEDDING_PROVIDER", "openai"),
        )


@dataclass(frozen=True)
class RedisConfig:
    url: str = ""
    password: str = ""
    db: int = 0
    key_prefix: str = "graphrag:cb:"

    @classmethod
    def from_env(cls) -> RedisConfig:
        return cls(
            url=os.environ.get("REDIS_URL", ""),
            password=os.environ.get("REDIS_PASSWORD", ""),
            db=int(os.environ.get("REDIS_DB", "0")),
            key_prefix=os.environ.get("REDIS_KEY_PREFIX", "graphrag:cb:"),
        )


@dataclass(frozen=True)
class RAGEvalConfig:
    low_relevance_threshold: float = 0.3
    enable_evaluation: bool = True
    use_llm_judge: bool = True

    @classmethod
    def from_env(cls) -> RAGEvalConfig:
        threshold_str = os.environ.get("RAG_LOW_RELEVANCE_THRESHOLD", "0.3")
        enable_str = os.environ.get("RAG_ENABLE_EVALUATION", "true").lower()
        llm_judge_str = os.environ.get("RAG_USE_LLM_JUDGE", "true").lower()
        return cls(
            low_relevance_threshold=float(threshold_str),
            enable_evaluation=enable_str in ("true", "1", "yes"),
            use_llm_judge=llm_judge_str in ("true", "1", "yes"),
        )
