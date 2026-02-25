from __future__ import annotations

import pytest

from orchestrator.app.entity_resolver import (
    EntityResolver,
    ScopedEntityId,
    resolve_entity_id,
)


class TestDeterministicCompositeKey:
    def test_same_inputs_produce_same_id(self) -> None:
        id_a = resolve_entity_id("auth", "repo-a", "prod")
        id_b = resolve_entity_id("auth", "repo-a", "prod")
        assert id_a == id_b

    def test_different_namespace_produces_different_id(self) -> None:
        id_a = resolve_entity_id("payment-service", "repo", "prod")
        id_b = resolve_entity_id("payments-service", "repo", "prod")
        assert id_a != id_b

    def test_different_repo_produces_different_id(self) -> None:
        id_a = resolve_entity_id("auth", "team-a/repo", "prod")
        id_b = resolve_entity_id("auth", "team-b/repo", "prod")
        assert id_a != id_b


class TestNoFuzzyMatching:
    def test_similar_names_different_namespaces_stay_separate(self) -> None:
        resolver = EntityResolver()
        r1 = resolver.resolve(
            name="payment-service",
            repository="repo",
            namespace="ns-a",
        )
        r2 = resolver.resolve(
            name="payments-service",
            repository="repo",
            namespace="ns-a",
        )
        assert r1.resolved_id != r2.resolved_id
        assert r2.is_new is True

    def test_payment_service_vs_payments_service_no_merge(self) -> None:
        resolver = EntityResolver()
        r1 = resolver.resolve(
            name="payment-service",
            repository="repo",
            namespace="prod",
        )
        r2 = resolver.resolve(
            name="payments-service",
            repository="repo",
            namespace="prod",
        )
        assert r1.resolved_id != r2.resolved_id

    def test_exact_match_still_merges(self) -> None:
        resolver = EntityResolver()
        r1 = resolver.resolve(
            name="auth-service",
            repository="repo",
            namespace="prod",
        )
        r2 = resolver.resolve(
            name="auth-service",
            repository="repo",
            namespace="prod",
        )
        assert r1.resolved_id == r2.resolved_id
        assert r2.is_new is False


class TestAliasRegistry:
    def test_alias_resolves_to_canonical(self) -> None:
        resolver = EntityResolver()
        resolver.register_alias("auth-svc", "auth-service")
        r1 = resolver.resolve(
            name="auth-service",
            repository="repo",
            namespace="prod",
        )
        r2 = resolver.resolve(
            name="auth-svc",
            repository="repo",
            namespace="prod",
        )
        assert r2.resolved_id == r1.resolved_id
        assert r2.is_new is False

    def test_unknown_alias_treated_as_new(self) -> None:
        resolver = EntityResolver()
        resolver.register_alias("auth-svc", "auth-service")
        result = resolver.resolve(
            name="billing-svc",
            repository="repo",
            namespace="prod",
        )
        assert result.is_new is True


class TestLRUBoundedMemory:
    def test_known_entities_bounded_by_max_size(self) -> None:
        resolver = EntityResolver(max_known=5)
        for i in range(10):
            resolver.resolve(
                name=f"service-{i}",
                repository="repo",
                namespace="prod",
            )
        assert len(resolver.known_entities) <= 5

    def test_lru_evicts_oldest(self) -> None:
        resolver = EntityResolver(max_known=3)
        resolver.resolve(name="svc-0", repository="r", namespace="n")
        resolver.resolve(name="svc-1", repository="r", namespace="n")
        resolver.resolve(name="svc-2", repository="r", namespace="n")
        resolver.resolve(name="svc-3", repository="r", namespace="n")
        known = resolver.known_entities
        first_key = resolve_entity_id("svc-0", "r", "n")
        assert first_key not in known


class TestLevenshteinRemoved:
    def test_no_levenshtein_function(self) -> None:
        import orchestrator.app.entity_resolver as er

        assert not hasattr(er, "_levenshtein"), (
            "_levenshtein should be removed"
        )

    def test_no_name_similarity_function(self) -> None:
        import orchestrator.app.entity_resolver as er

        assert not hasattr(er, "name_similarity"), (
            "name_similarity should be removed"
        )
