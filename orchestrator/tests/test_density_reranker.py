from __future__ import annotations

import os
from unittest import mock

from orchestrator.app.reranker import Reranker
from orchestrator.app.density_reranker import (
    DensityReranker,
    DensityRerankerConfig,
    _jaccard_similarity,
)


class TestDensityRerankerProtocol:
    def test_density_reranker_protocol_compliance(self) -> None:
        reranker = DensityReranker()
        assert isinstance(reranker, Reranker)


class TestDensityRerankerDiversification:
    def test_density_reranker_diversifies_redundant_candidates(self) -> None:
        candidates = [
            {"name": "auth-service", "id": "auth-1", "description": "handles user authentication and login"},
            {"name": "auth-proxy", "id": "auth-2", "description": "handles user authentication and session"},
            {"name": "auth-gateway", "id": "auth-3", "description": "handles user authentication and tokens"},
            {"name": "billing-engine", "id": "billing-1", "description": "processes payment transactions"},
            {"name": "kafka-broker", "id": "kafka-1", "description": "message broker for event streaming"},
        ]

        pure_relevance = DensityReranker(lambda_param=1.0)
        relevance_result = pure_relevance.rerank("authentication service login", candidates)
        relevance_order = [sc.data["id"] for sc in relevance_result]

        diverse = DensityReranker(lambda_param=0.5)
        diverse_result = diverse.rerank("authentication service login", candidates)
        diverse_order = [sc.data["id"] for sc in diverse_result]

        non_auth_best_diverse = min(
            (i for i, cid in enumerate(diverse_order) if not cid.startswith("auth")),
            default=len(diverse_order),
        )
        non_auth_best_relevance = min(
            (i for i, cid in enumerate(relevance_order) if not cid.startswith("auth")),
            default=len(relevance_order),
        )

        assert non_auth_best_diverse < non_auth_best_relevance

    def test_density_reranker_preserves_order_for_diverse_candidates(self) -> None:
        candidates = [
            {"name": "auth-service", "id": "auth-1", "description": "handles user authentication"},
            {"name": "billing-engine", "id": "billing-1", "description": "processes payment transactions"},
            {"name": "kafka-broker", "id": "kafka-1", "description": "message broker for event streaming"},
            {"name": "neo4j-store", "id": "neo4j-1", "description": "graph database storage engine"},
            {"name": "api-gateway", "id": "api-1", "description": "http routing and load balancing"},
        ]

        relevance_only = DensityReranker(lambda_param=1.0)
        relevance_result = relevance_only.rerank("distributed systems query", candidates)
        relevance_ids = [sc.data["id"] for sc in relevance_result]

        diverse = DensityReranker(lambda_param=0.7)
        diverse_result = diverse.rerank("distributed systems query", candidates)
        diverse_ids = [sc.data["id"] for sc in diverse_result]

        assert diverse_ids[0] == relevance_ids[0]


class TestDensityRerankerLambdaBounds:
    def test_density_reranker_lambda_zero_pure_diversity(self) -> None:
        candidates = [
            {"name": "auth-service", "id": "auth-1", "description": "handles authentication and login"},
            {"name": "auth-proxy", "id": "auth-2", "description": "handles authentication and session"},
            {"name": "billing-engine", "id": "billing-1", "description": "processes payments"},
        ]
        reranker = DensityReranker(lambda_param=0.0)
        result = reranker.rerank("authentication service", candidates)

        result_ids = [sc.data["id"] for sc in result]
        auth_indices = [i for i, cid in enumerate(result_ids) if cid.startswith("auth")]
        assert auth_indices != [0, 1], "Pure diversity should not place near-duplicates adjacent"

    def test_density_reranker_lambda_one_pure_relevance(self) -> None:
        candidates = [
            {"name": "auth-service", "id": "auth-1", "description": "handles user authentication"},
            {"name": "billing-engine", "id": "billing-1", "description": "processes payments"},
            {"name": "auth-proxy", "id": "auth-2", "description": "handles user authentication proxy"},
        ]
        reranker_relevance = DensityReranker(lambda_param=1.0)
        from orchestrator.app.reranker import BM25Reranker
        bm25 = BM25Reranker()

        rel_result = reranker_relevance.rerank("authentication", candidates)
        bm25_result = bm25.rerank("authentication", candidates)

        rel_ids = [sc.data["id"] for sc in rel_result]
        bm25_ids = [sc.data["id"] for sc in bm25_result]

        assert rel_ids == bm25_ids


class TestDensityRerankerEdgeCases:
    def test_density_reranker_empty_candidates(self) -> None:
        reranker = DensityReranker()
        result = reranker.rerank("query", [])
        assert result == []

    def test_density_reranker_single_candidate(self) -> None:
        reranker = DensityReranker()
        candidates = [{"name": "auth-service", "id": "auth-1"}]
        result = reranker.rerank("auth", candidates)
        assert len(result) == 1
        assert result[0].data == candidates[0]

    def test_density_reranker_skips_below_min_candidates(self) -> None:
        config = DensityRerankerConfig(min_candidates=5)
        reranker = DensityReranker(
            lambda_param=config.lambda_param,
            min_candidates=config.min_candidates,
        )
        candidates = [
            {"name": "auth-service", "id": "auth-1", "description": "authentication"},
            {"name": "billing-engine", "id": "billing-1", "description": "payments"},
        ]
        from orchestrator.app.reranker import BM25Reranker
        bm25 = BM25Reranker()

        result = reranker.rerank("auth", candidates)
        bm25_result = bm25.rerank("auth", candidates)

        result_ids = [sc.data["id"] for sc in result]
        bm25_ids = [sc.data["id"] for sc in bm25_result]
        assert result_ids == bm25_ids


class TestDensityRerankerConfig:
    def test_density_reranker_config_defaults(self) -> None:
        config = DensityRerankerConfig()
        assert config.lambda_param == 0.7
        assert config.min_candidates == 3
        assert config.enable_density_rerank is True

    def test_density_reranker_config_from_env(self) -> None:
        env_vars = {
            "DENSITY_RERANK_LAMBDA": "0.5",
            "DENSITY_RERANK_MIN_CANDIDATES": "10",
            "DENSITY_RERANK_ENABLED": "false",
        }
        with mock.patch.dict(os.environ, env_vars, clear=False):
            config = DensityRerankerConfig.from_env()
        assert config.lambda_param == 0.5
        assert config.min_candidates == 10
        assert config.enable_density_rerank is False


class TestJaccardSimilarity:
    def test_jaccard_similarity_identical(self) -> None:
        tokens = {"auth", "service", "login"}
        assert _jaccard_similarity(tokens, tokens) == 1.0

    def test_jaccard_similarity_disjoint(self) -> None:
        tokens_a = {"auth", "service"}
        tokens_b = {"billing", "payment"}
        assert _jaccard_similarity(tokens_a, tokens_b) == 0.0

    def test_jaccard_similarity_partial_overlap(self) -> None:
        tokens_a = {"auth", "service", "login"}
        tokens_b = {"auth", "service", "proxy"}
        result = _jaccard_similarity(tokens_a, tokens_b)
        assert result == 2.0 / 4.0
