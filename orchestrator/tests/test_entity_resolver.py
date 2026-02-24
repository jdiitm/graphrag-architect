from __future__ import annotations

import pytest

from orchestrator.app.entity_resolver import (
    EntityResolver,
    ScopedEntityId,
    compute_similarity,
    name_similarity,
    normalize_name,
    resolve_entity_id,
)


class TestScopedEntityId:
    def test_format(self) -> None:
        scoped = ScopedEntityId(
            repository="team-a/auth-service",
            namespace="backend",
            name="auth",
        )
        assert str(scoped) == "team-a/auth-service::backend::auth"

    def test_from_string(self) -> None:
        scoped = ScopedEntityId.from_string(
            "team-a/auth-service::backend::auth"
        )
        assert scoped.repository == "team-a/auth-service"
        assert scoped.namespace == "backend"
        assert scoped.name == "auth"

    def test_legacy_unscoped_id(self) -> None:
        scoped = ScopedEntityId.from_string("auth")
        assert scoped.repository == ""
        assert scoped.namespace == ""
        assert scoped.name == "auth"


class TestResolveEntityId:
    def test_different_repos_stay_separate(self) -> None:
        id_a = resolve_entity_id(
            name="auth", repository="team-a/repo", namespace="prod",
        )
        id_b = resolve_entity_id(
            name="auth", repository="team-b/repo", namespace="prod",
        )
        assert id_a != id_b

    def test_same_repo_same_name_merge(self) -> None:
        id_a = resolve_entity_id(
            name="auth", repository="team-a/repo", namespace="prod",
        )
        id_b = resolve_entity_id(
            name="auth", repository="team-a/repo", namespace="prod",
        )
        assert id_a == id_b

    def test_empty_repo_falls_back_to_name(self) -> None:
        entity_id = resolve_entity_id(
            name="standalone", repository="", namespace="",
        )
        assert "standalone" in entity_id


class TestComputeSimilarity:
    def test_identical_entities_score_one(self) -> None:
        attrs_a = {"name": "auth", "language": "python", "framework": "fastapi"}
        attrs_b = {"name": "auth", "language": "python", "framework": "fastapi"}
        assert compute_similarity(attrs_a, attrs_b) == 1.0

    def test_completely_different_score_low(self) -> None:
        attrs_a = {"name": "auth", "language": "python"}
        attrs_b = {"name": "billing", "language": "go"}
        score = compute_similarity(attrs_a, attrs_b)
        assert score < 0.5

    def test_partial_overlap(self) -> None:
        attrs_a = {"name": "auth", "language": "python", "framework": "fastapi"}
        attrs_b = {"name": "auth", "language": "go", "framework": "gin"}
        score = compute_similarity(attrs_a, attrs_b)
        assert 0.0 < score < 1.0


class TestEntityResolver:
    def test_new_entity_added(self) -> None:
        resolver = EntityResolver(threshold=0.85)
        result = resolver.resolve(
            name="auth",
            repository="team-a/repo",
            namespace="prod",
            attributes={"language": "python"},
        )
        assert result.is_new is True
        assert "team-a/repo" in result.resolved_id

    def test_duplicate_entity_merged(self) -> None:
        resolver = EntityResolver(threshold=0.85)
        resolver.resolve(
            name="auth",
            repository="team-a/repo",
            namespace="prod",
            attributes={"language": "python", "framework": "fastapi"},
        )
        result = resolver.resolve(
            name="auth",
            repository="team-a/repo",
            namespace="prod",
            attributes={"language": "python", "framework": "fastapi"},
        )
        assert result.is_new is False

    def test_same_name_different_repo_separate(self) -> None:
        resolver = EntityResolver(threshold=0.85)
        r1 = resolver.resolve(
            name="auth",
            repository="team-a/repo",
            namespace="prod",
            attributes={"language": "python"},
        )
        r2 = resolver.resolve(
            name="auth",
            repository="team-b/repo",
            namespace="prod",
            attributes={"language": "go"},
        )
        assert r1.resolved_id != r2.resolved_id
        assert r2.is_new is True

    def test_provenance_tracked(self) -> None:
        resolver = EntityResolver(threshold=0.85)
        resolver.resolve(
            name="auth",
            repository="team-a/repo",
            namespace="prod",
            attributes={"language": "python"},
        )
        result = resolver.resolve(
            name="auth",
            repository="team-a/repo",
            namespace="prod",
            attributes={"language": "python"},
        )
        assert result.resolved_from is not None


class TestNormalizeName:
    def test_strips_hyphens_and_lowercases(self) -> None:
        assert normalize_name("payment-api") == "paymentapi"

    def test_strips_underscores(self) -> None:
        assert normalize_name("payments_service") == "paymentsservice"

    def test_strips_dots(self) -> None:
        assert normalize_name("payments.service") == "paymentsservice"

    def test_empty_string(self) -> None:
        assert normalize_name("") == ""


class TestNameSimilarity:
    def test_identical_names(self) -> None:
        assert name_similarity("auth-service", "auth-service") == 1.0

    def test_hyphen_vs_underscore_equivalent(self) -> None:
        assert name_similarity("payment-api", "payment_api") == 1.0

    def test_similar_names_high_score(self) -> None:
        score = name_similarity("payment-api", "payments-api")
        assert score > 0.80

    def test_different_names_low_score(self) -> None:
        score = name_similarity("auth-service", "billing-engine")
        assert score < 0.5


class TestCrossRepoResolution:
    def test_fuzzy_match_across_repos(self) -> None:
        resolver = EntityResolver(threshold=0.75, name_similarity_threshold=0.80)
        r1 = resolver.resolve(
            name="payment-api",
            repository="team-a/payments",
            namespace="prod",
            attributes={"language": "go"},
        )
        assert r1.is_new is True

        r2 = resolver.resolve(
            name="payment_api",
            repository="team-b/checkout",
            namespace="prod",
            attributes={"language": "go"},
        )
        assert r2.is_new is False
        assert r2.resolved_id == r1.resolved_id

    def test_no_false_positives_different_services(self) -> None:
        resolver = EntityResolver(threshold=0.85, name_similarity_threshold=0.80)
        r1 = resolver.resolve(
            name="auth-service",
            repository="team-a/auth",
            namespace="prod",
            attributes={"language": "python", "framework": "fastapi"},
        )
        r2 = resolver.resolve(
            name="billing-engine",
            repository="team-b/billing",
            namespace="prod",
            attributes={"language": "go", "framework": "gin"},
        )
        assert r2.is_new is True
        assert r2.resolved_id != r1.resolved_id


class TestResolveEntities:
    def test_deduplicates_similar_service_nodes(self) -> None:
        from orchestrator.app.extraction_models import ServiceNode

        entities = [
            ServiceNode(
                id="payment-api",
                name="payment-api",
                language="go",
                framework="gin",
                opentelemetry_enabled=True,
                tenant_id="test-tenant",
            ),
            ServiceNode(
                id="payment_api",
                name="payment_api",
                language="go",
                framework="gin",
                opentelemetry_enabled=False,
                tenant_id="test-tenant",
            ),
        ]
        resolver = EntityResolver(threshold=0.85, name_similarity_threshold=1.0)
        resolved = resolver.resolve_entities(entities)
        service_nodes = [e for e in resolved if isinstance(e, ServiceNode)]
        assert len(service_nodes) == 1

    def test_preserves_distinct_services(self) -> None:
        from orchestrator.app.extraction_models import ServiceNode

        entities = [
            ServiceNode(
                id="auth-service",
                name="auth-service",
                language="python",
                framework="fastapi",
                opentelemetry_enabled=True,
                tenant_id="test-tenant",
            ),
            ServiceNode(
                id="billing-engine",
                name="billing-engine",
                language="go",
                framework="gin",
                opentelemetry_enabled=True,
                tenant_id="test-tenant",
            ),
        ]
        resolver = EntityResolver(threshold=0.85, name_similarity_threshold=0.80)
        resolved = resolver.resolve_entities(entities)
        service_nodes = [e for e in resolved if isinstance(e, ServiceNode)]
        assert len(service_nodes) == 2
