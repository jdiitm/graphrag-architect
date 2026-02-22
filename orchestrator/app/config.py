from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Neo4jConfig:
    uri: str
    username: str
    password: str

    @classmethod
    def from_env(cls) -> Neo4jConfig:
        return cls(
            uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
            username=os.environ.get("NEO4J_USERNAME", "neo4j"),
            password=os.environ["NEO4J_PASSWORD"],
        )


@dataclass(frozen=True)
class ExtractionConfig:
    google_api_key: str
    model_name: str = "gemini-2.0-flash"
    max_concurrency: int = 5
    token_budget_per_batch: int = 200_000
    max_retries: int = 5
    retry_min_wait: float = 1.0
    retry_max_wait: float = 60.0

    @classmethod
    def from_env(cls) -> ExtractionConfig:
        return cls(
            google_api_key=os.environ["GOOGLE_API_KEY"],
            model_name=os.environ.get("EXTRACTION_MODEL", "gemini-2.0-flash"),
            max_concurrency=int(os.environ.get("EXTRACTION_MAX_CONCURRENCY", "5")),
            token_budget_per_batch=int(
                os.environ.get("EXTRACTION_TOKEN_BUDGET", "200000")
            ),
        )
