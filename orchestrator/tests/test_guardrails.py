from __future__ import annotations

import pytest

from orchestrator.app.ontology import Ontology, NodeTypeDefinition, EdgeTypeDefinition
from orchestrator.app.guardrails import (
    CypherSchemaValidator,
    CypherComplexityGuard,
    ResponseCoherenceChecker,
    GuardrailChain,
    GuardrailMode,
    GuardrailViolation,
)


def _test_ontology() -> Ontology:
    return Ontology(
        node_types={
            "Service": NodeTypeDefinition(
                properties={"id": "string", "name": "string"},
                unique_key="id",
            ),
            "KafkaTopic": NodeTypeDefinition(
                properties={"name": "string"},
                unique_key="name",
            ),
        },
        edge_types={
            "CALLS": EdgeTypeDefinition(source_type="Service", target_type="Service"),
            "PRODUCES": EdgeTypeDefinition(source_type="Service", target_type="KafkaTopic"),
        },
    )


class TestCypherSchemaValidator:

    def test_accepts_valid_labels(self) -> None:
        ontology = _test_ontology()
        validator = CypherSchemaValidator(ontology)
        violations = validator.validate("MATCH (n:Service)-[:CALLS]->(m:Service) RETURN n")
        assert len(violations) == 0

    def test_rejects_unknown_label(self) -> None:
        ontology = _test_ontology()
        validator = CypherSchemaValidator(ontology)
        violations = validator.validate("MATCH (n:UnknownType) RETURN n")
        assert len(violations) >= 1
        assert "UnknownType" in violations[0].detail

    def test_rejects_unknown_relationship(self) -> None:
        ontology = _test_ontology()
        validator = CypherSchemaValidator(ontology)
        violations = validator.validate("MATCH (n:Service)-[:FAKE_REL]->(m:Service) RETURN n")
        assert len(violations) >= 1
        assert "FAKE_REL" in violations[0].detail


class TestCypherComplexityGuard:

    def test_rejects_too_many_match_clauses(self) -> None:
        guard = CypherComplexityGuard(max_match_clauses=3)
        cypher = " ".join(f"MATCH (n{i}:Service)" for i in range(5)) + " RETURN n0"
        violations = guard.validate(cypher)
        assert len(violations) >= 1

    def test_accepts_within_limit(self) -> None:
        guard = CypherComplexityGuard(max_match_clauses=5)
        cypher = "MATCH (n:Service) RETURN n"
        violations = guard.validate(cypher)
        assert len(violations) == 0


class TestResponseCoherenceChecker:

    def test_flags_entity_not_in_context(self) -> None:
        checker = ResponseCoherenceChecker()
        context_entities = ["auth-svc", "kafka-topic-events"]
        answer = "The payment-svc depends on auth-svc"
        violations = checker.validate(answer, context_entities)
        assert len(violations) >= 1

    def test_passes_when_all_entities_in_context(self) -> None:
        checker = ResponseCoherenceChecker()
        context_entities = ["auth-svc", "kafka-topic-events"]
        answer = "The auth-svc produces to kafka-topic-events"
        violations = checker.validate(answer, context_entities)
        assert len(violations) == 0


class TestGuardrailChain:

    def test_chain_aggregates_violations(self) -> None:
        ontology = _test_ontology()
        chain = GuardrailChain(
            schema_validator=CypherSchemaValidator(ontology),
            complexity_guard=CypherComplexityGuard(max_match_clauses=2),
        )
        cypher = "MATCH (n:FakeLabel) MATCH (m:Service) MATCH (k:Service) RETURN n"
        violations = chain.validate_cypher(cypher)
        assert len(violations) >= 2

    def test_warn_mode_does_not_raise(self) -> None:
        ontology = _test_ontology()
        chain = GuardrailChain(
            schema_validator=CypherSchemaValidator(ontology),
            mode=GuardrailMode.WARN,
        )
        cypher = "MATCH (n:FakeLabel) RETURN n"
        violations = chain.validate_cypher(cypher)
        assert len(violations) >= 1
