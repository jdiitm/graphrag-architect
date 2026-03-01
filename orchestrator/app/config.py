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
    max_connection_pool_size: int = 100
    connection_acquisition_timeout: float = 60.0

    @classmethod
    def from_env(cls) -> Neo4jConfig:
        return cls(
            uri=os.environ.get("NEO4J_URI", "neo4j://localhost:7687"),
            username=os.environ.get("NEO4J_USERNAME", "neo4j"),
            password=os.environ["NEO4J_PASSWORD"],
            query_timeout=float(os.environ.get("NEO4J_QUERY_TIMEOUT", "30")),
            database=os.environ.get("NEO4J_DATABASE", "neo4j"),
            max_connection_pool_size=int(
                os.environ.get("NEO4J_MAX_CONNECTION_POOL_SIZE", "100"),
            ),
            connection_acquisition_timeout=float(
                os.environ.get("NEO4J_CONNECTION_ACQUISITION_TIMEOUT", "60"),
            ),
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
    anthropic_api_key: str = ""
    claude_model_name: str = "claude-sonnet-4-20250514"

    @classmethod
    def from_env(cls) -> ExtractionConfig:
        return cls(
            google_api_key=os.environ.get("GOOGLE_API_KEY", ""),
            model_name=os.environ.get("EXTRACTION_MODEL", "gemini-2.0-flash"),
            max_concurrency=int(os.environ.get("EXTRACTION_MAX_CONCURRENCY", "5")),
            token_budget_per_batch=int(
                os.environ.get("EXTRACTION_TOKEN_BUDGET", "200000")
            ),
            max_retries=int(os.environ.get("EXTRACTION_MAX_RETRIES", "5")),
            retry_min_wait=float(os.environ.get("EXTRACTION_RETRY_MIN_WAIT", "1.0")),
            retry_max_wait=float(os.environ.get("EXTRACTION_RETRY_MAX_WAIT", "60.0")),
            llm_provider=os.environ.get("LLM_PROVIDER", "gemini"),
            anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
            claude_model_name=os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514"),
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
    pool_size: int = 4
    deployment_mode: str = "dev"

    @classmethod
    def from_env(cls) -> VectorStoreConfig:
        return cls(
            backend=os.environ.get("VECTOR_STORE_BACKEND", "memory"),
            qdrant_url=os.environ.get("QDRANT_URL", ""),
            qdrant_api_key=os.environ.get("QDRANT_API_KEY", ""),
            pool_size=int(os.environ.get("QDRANT_POOL_SIZE", "4")),
            deployment_mode=os.environ.get("DEPLOYMENT_MODE", "dev").lower(),
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


@dataclass(frozen=True)
class JobStoreConfig:
    ttl_seconds: float = 3600.0

    @classmethod
    def from_env(cls) -> JobStoreConfig:
        raw = os.environ.get("JOB_STORE_TTL_SECONDS", "")
        if raw:
            return cls(ttl_seconds=float(raw))
        return cls()


@dataclass(frozen=True)
class ReadReplicaConfig:
    enabled: bool = False
    read_replica_uris: tuple[str, ...] = ()
    read_pool_size: int = 50

    @classmethod
    def from_env(cls) -> ReadReplicaConfig:
        enabled = os.environ.get(
            "NEO4J_READ_REPLICA_ENABLED", "false",
        ).lower() in ("true", "1", "yes")
        raw_uris = os.environ.get("NEO4J_READ_REPLICA_URIS", "")
        uris = tuple(
            u.strip() for u in raw_uris.split(",") if u.strip()
        )
        pool_size = int(
            os.environ.get("NEO4J_READ_REPLICA_POOL_SIZE", "50"),
        )
        return cls(
            enabled=enabled,
            read_replica_uris=uris,
            read_pool_size=pool_size,
        )


_SINK_BATCH_MIN = 100
_SINK_BATCH_MAX = 5000
_SINK_BATCH_DEFAULT = 500


@dataclass(frozen=True)
class IngestionConfig:
    sink_batch_size: int = _SINK_BATCH_DEFAULT

    @classmethod
    def from_env(cls) -> IngestionConfig:
        raw = os.environ.get("SINK_BATCH_SIZE", "")
        if raw:
            value = max(_SINK_BATCH_MIN, min(_SINK_BATCH_MAX, int(raw)))
        else:
            value = _SINK_BATCH_DEFAULT
        return cls(sink_batch_size=value)


@dataclass(frozen=True)
class ASTPoolConfig:
    ceiling: int = 8

    @classmethod
    def from_env(cls) -> ASTPoolConfig:
        raw = os.environ.get("AST_POOL_CEILING", "")
        if raw:
            raw_value = int(raw)
        else:
            raw_value = 8
        cpu_count = os.cpu_count() or 4
        clamped = max(1, min(cpu_count * 2, raw_value))
        return cls(ceiling=clamped)


@dataclass(frozen=True)
class GRPCASTConfig:
    endpoint: str = ""
    timeout_seconds: float = 30.0
    max_retries: int = 3

    @classmethod
    def from_env(cls) -> GRPCASTConfig:
        return cls(
            endpoint=os.environ.get("AST_GRPC_ENDPOINT", ""),
            timeout_seconds=float(os.environ.get("AST_GRPC_TIMEOUT", "30")),
            max_retries=int(os.environ.get("AST_GRPC_MAX_RETRIES", "3")),
        )


@dataclass(frozen=True)
class HotTargetConfig:
    hot_target_threshold: int = 10
    hot_target_max_concurrent: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "hot_target_threshold",
            max(1, self.hot_target_threshold),
        )
        object.__setattr__(
            self, "hot_target_max_concurrent",
            max(1, self.hot_target_max_concurrent),
        )

    @classmethod
    def from_env(cls) -> HotTargetConfig:
        return cls(
            hot_target_threshold=int(
                os.environ.get("HOT_TARGET_THRESHOLD", "10"),
            ),
            hot_target_max_concurrent=int(
                os.environ.get("HOT_TARGET_MAX_CONCURRENT", "1"),
            ),
        )


@dataclass(frozen=True)
class SchemaStoreConfig:
    backend: str = "memory"

    @classmethod
    def from_env(cls) -> SchemaStoreConfig:
        return cls(
            backend=os.environ.get("SCHEMA_STORE_BACKEND", "memory"),
        )


@dataclass(frozen=True)
class ContextRankingConfig:
    rerank_timeout_seconds: float = 5.0
    truncation_timeout_seconds: float = 3.0

    @classmethod
    def from_env(cls) -> ContextRankingConfig:
        raw_rerank = os.environ.get("RERANK_TIMEOUT_SECONDS", "")
        raw_trunc = os.environ.get("TRUNCATION_TIMEOUT_SECONDS", "")
        return cls(
            rerank_timeout_seconds=float(raw_rerank) if raw_rerank else 5.0,
            truncation_timeout_seconds=float(raw_trunc) if raw_trunc else 3.0,
        )


@dataclass(frozen=True)
class VectorSyncConfig:
    backend: str = "memory"
    kafka_topic: str = "graph.mutations"

    @classmethod
    def from_env(cls) -> VectorSyncConfig:
        return cls(
            backend=os.environ.get("VECTOR_SYNC_BACKEND", "memory"),
            kafka_topic=os.environ.get(
                "VECTOR_SYNC_KAFKA_TOPIC", "graph.mutations",
            ),
        )
