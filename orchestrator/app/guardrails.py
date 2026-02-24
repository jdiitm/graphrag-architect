from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from orchestrator.app.ontology import Ontology

logger = logging.getLogger(__name__)


class GuardrailMode(Enum):
    REJECT = "reject"
    WARN = "warn"
    LOG = "log"


@dataclass(frozen=True)
class GuardrailViolation:
    guardrail: str
    detail: str
    severity: str = "error"


_LABEL_PATTERN = re.compile(r"(?<!\[):([A-Z][A-Za-z0-9_]*)")
_REL_TYPE_PATTERN = re.compile(r"\[:([A-Z_]+)")
_MATCH_PATTERN = re.compile(r"\bMATCH\b", re.IGNORECASE)
_OPTIONAL_MATCH_PATTERN = re.compile(r"\bOPTIONAL\s+MATCH\b", re.IGNORECASE)

_CYPHER_KEYWORDS = frozenset({
    "MATCH", "WHERE", "RETURN", "WITH", "OPTIONAL", "ORDER", "BY",
    "SKIP", "LIMIT", "UNION", "UNWIND", "CREATE", "DELETE", "SET",
    "MERGE", "CALL", "YIELD", "DETACH", "REMOVE", "FOREACH", "CASE",
    "WHEN", "THEN", "ELSE", "END", "AND", "OR", "NOT", "IN", "AS",
    "IS", "NULL", "TRUE", "FALSE", "EXISTS", "ALL", "ANY", "NONE",
    "SINGLE", "DISTINCT", "COUNT", "COLLECT", "EXPLAIN", "PROFILE",
})


class CypherSchemaValidator:
    def __init__(self, ontology: Ontology) -> None:
        self._ontology = ontology
        self._valid_labels = frozenset(ontology.node_types.keys())
        self._valid_rels = frozenset(ontology.edge_types.keys())

    def validate(self, cypher: str) -> List[GuardrailViolation]:
        violations: List[GuardrailViolation] = []

        labels = set(_LABEL_PATTERN.findall(cypher))
        for label in labels:
            if label in _CYPHER_KEYWORDS or label.upper() in _CYPHER_KEYWORDS:
                continue
            if label not in self._valid_labels:
                violations.append(GuardrailViolation(
                    guardrail="CypherSchemaValidator",
                    detail=f"Unknown node label: {label}",
                ))

        rel_types = set(_REL_TYPE_PATTERN.findall(cypher))
        for rel_type in rel_types:
            if rel_type not in self._valid_rels:
                violations.append(GuardrailViolation(
                    guardrail="CypherSchemaValidator",
                    detail=f"Unknown relationship type: {rel_type}",
                ))

        return violations


class CypherComplexityGuard:
    def __init__(
        self,
        max_match_clauses: int = 5,
        max_optional_match: int = 3,
    ) -> None:
        self._max_match = max_match_clauses
        self._max_optional = max_optional_match

    def validate(self, cypher: str) -> List[GuardrailViolation]:
        violations: List[GuardrailViolation] = []

        match_count = len(_MATCH_PATTERN.findall(cypher))
        optional_count = len(_OPTIONAL_MATCH_PATTERN.findall(cypher))
        plain_match = match_count - optional_count

        if plain_match > self._max_match:
            violations.append(GuardrailViolation(
                guardrail="CypherComplexityGuard",
                detail=(
                    f"Query has {plain_match} MATCH clauses "
                    f"(max {self._max_match})"
                ),
            ))

        if optional_count > self._max_optional:
            violations.append(GuardrailViolation(
                guardrail="CypherComplexityGuard",
                detail=(
                    f"Query has {optional_count} OPTIONAL MATCH clauses "
                    f"(max {self._max_optional})"
                ),
            ))

        return violations


class ResponseCoherenceChecker:
    def validate(
        self,
        answer: str,
        context_entities: List[str],
    ) -> List[GuardrailViolation]:
        violations: List[GuardrailViolation] = []
        if not context_entities:
            return violations

        words = set(re.findall(r"[a-zA-Z0-9_-]+", answer))
        context_set = set(context_entities)

        for word in words:
            if (
                "-" in word
                and word not in context_set
                and any(c.isalpha() for c in word)
                and len(word) > 3
            ):
                if word.lower() not in {e.lower() for e in context_set}:
                    possible_entity = word
                    if any(
                        possible_entity.endswith(suf)
                        for suf in ("-svc", "-service", "-api", "-topic", "-db")
                    ):
                        violations.append(GuardrailViolation(
                            guardrail="ResponseCoherenceChecker",
                            detail=(
                                f"Entity '{possible_entity}' referenced in answer "
                                "but not present in retrieved context"
                            ),
                            severity="warning",
                        ))

        return violations


class GuardrailChain:
    def __init__(
        self,
        schema_validator: Optional[CypherSchemaValidator] = None,
        complexity_guard: Optional[CypherComplexityGuard] = None,
        coherence_checker: Optional[ResponseCoherenceChecker] = None,
        mode: GuardrailMode = GuardrailMode.REJECT,
    ) -> None:
        self._schema = schema_validator
        self._complexity = complexity_guard
        self._coherence = coherence_checker
        self._mode = mode

    def validate_cypher(self, cypher: str) -> List[GuardrailViolation]:
        violations: List[GuardrailViolation] = []
        if self._schema:
            violations.extend(self._schema.validate(cypher))
        if self._complexity:
            violations.extend(self._complexity.validate(cypher))
        if violations and self._mode == GuardrailMode.WARN:
            for v in violations:
                logger.warning("Guardrail violation [%s]: %s", v.guardrail, v.detail)
        return violations

    def validate_response(
        self,
        answer: str,
        context_entities: List[str],
    ) -> List[GuardrailViolation]:
        if self._coherence:
            return self._coherence.validate(answer, context_entities)
        return []
