from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from orchestrator.app.extraction_models import ServiceNode


@dataclass(frozen=True)
class ScopedEntityId:
    repository: str
    namespace: str
    name: str

    def __str__(self) -> str:
        if self.repository or self.namespace:
            return f"{self.repository}::{self.namespace}::{self.name}"
        return self.name

    @classmethod
    def from_string(cls, raw: str) -> ScopedEntityId:
        parts = raw.split("::")
        if len(parts) == 3:
            return cls(
                repository=parts[0], namespace=parts[1], name=parts[2],
            )
        return cls(repository="", namespace="", name=raw)


def resolve_entity_id(
    name: str, repository: str = "", namespace: str = "",
) -> str:
    scoped = ScopedEntityId(
        repository=repository, namespace=namespace, name=name,
    )
    return str(scoped)


_SEPARATOR_PATTERN = re.compile(r"[-_.\s]+")


def normalize_name(name: str) -> str:
    return _SEPARATOR_PATTERN.sub("", name).lower()


def name_similarity(name_a: str, name_b: str) -> float:
    norm_a = normalize_name(name_a)
    norm_b = normalize_name(name_b)
    if norm_a == norm_b:
        return 1.0
    if not norm_a or not norm_b:
        return 0.0
    max_len = max(len(norm_a), len(norm_b))
    distance = _levenshtein(norm_a, norm_b)
    return 1.0 - (distance / max_len)


def _levenshtein(s: str, t: str) -> int:
    if len(s) < len(t):
        return _levenshtein(s=t, t=s)
    if not t:
        return len(s)
    prev_row = list(range(len(t) + 1))
    for i, sc in enumerate(s):
        curr_row = [i + 1]
        for j, tc in enumerate(t):
            cost = 0 if sc == tc else 1
            curr_row.append(
                min(prev_row[j + 1] + 1, curr_row[j] + 1, prev_row[j] + cost)
            )
        prev_row = curr_row
    return prev_row[-1]


def compute_similarity(
    attrs_a: Dict[str, Any], attrs_b: Dict[str, Any],
) -> float:
    all_keys = set(attrs_a.keys()) | set(attrs_b.keys())
    if not all_keys:
        return 1.0
    matching = sum(
        1 for k in all_keys
        if attrs_a.get(k) == attrs_b.get(k)
    )
    return matching / len(all_keys)


@dataclass
class ResolutionResult:
    resolved_id: str
    is_new: bool
    resolved_from: Optional[str] = None


class EntityResolver:
    def __init__(
        self,
        threshold: float = 0.85,
        name_similarity_threshold: float = 1.0,
    ) -> None:
        self._threshold = threshold
        self._name_threshold = name_similarity_threshold
        self._known: Dict[str, Dict[str, Any]] = {}

    @property
    def known_entities(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._known)

    def resolve(
        self,
        name: str,
        repository: str = "",
        namespace: str = "",
        attributes: Optional[Dict[str, Any]] = None,
    ) -> ResolutionResult:
        entity_id = resolve_entity_id(name, repository, namespace)
        attrs = attributes or {}

        if entity_id in self._known:
            existing_attrs = self._known[entity_id]
            similarity = compute_similarity(attrs, existing_attrs)
            if similarity >= self._threshold:
                return ResolutionResult(
                    resolved_id=entity_id,
                    is_new=False,
                    resolved_from=entity_id,
                )

        fuzzy_match = self._find_cross_repo_match(name, attrs)
        if fuzzy_match is not None:
            canonical_id, _ = fuzzy_match
            self._known[entity_id] = dict(attrs)
            return ResolutionResult(
                resolved_id=canonical_id,
                is_new=False,
                resolved_from=canonical_id,
            )

        self._known[entity_id] = dict(attrs)
        return ResolutionResult(resolved_id=entity_id, is_new=True)

    def _find_cross_repo_match(
        self,
        name: str,
        attrs: Dict[str, Any],
    ) -> Optional[Tuple[str, float]]:
        best_id: Optional[str] = None
        best_score: float = 0.0
        for known_id, known_attrs in self._known.items():
            scoped = ScopedEntityId.from_string(known_id)
            n_sim = name_similarity(name, scoped.name)
            if n_sim < self._name_threshold:
                continue
            a_sim = compute_similarity(attrs, known_attrs)
            combined = 0.7 * n_sim + 0.3 * a_sim
            if combined >= self._threshold and combined > best_score:
                best_id = known_id
                best_score = combined
        if best_id is not None:
            return (best_id, best_score)
        return None

    def resolve_entities(self, entities: List[Any]) -> List[Any]:
        id_mapping: Dict[str, str] = {}
        for entity in entities:
            if not isinstance(entity, ServiceNode):
                continue
            result = self.resolve(
                name=entity.name or entity.id,
                attributes={
                    "language": entity.language,
                    "framework": entity.framework,
                },
            )
            if not result.is_new and result.resolved_id != entity.id:
                id_mapping[entity.id] = result.resolved_id

        if not id_mapping:
            return entities

        resolved: List[Any] = []
        seen_ids: set = set()
        for entity in entities:
            if isinstance(entity, ServiceNode):
                new_id = id_mapping.get(entity.id, entity.id)
                if new_id in seen_ids:
                    continue
                seen_ids.add(new_id)
                if new_id != entity.id:
                    entity = ServiceNode(
                        id=new_id,
                        name=entity.name,
                        language=entity.language,
                        framework=entity.framework,
                        opentelemetry_enabled=entity.opentelemetry_enabled,
                        tenant_id=entity.tenant_id,
                        team_owner=entity.team_owner,
                        namespace_acl=entity.namespace_acl,
                        confidence=entity.confidence,
                    )
                resolved.append(entity)
            else:
                resolved.append(entity)
        return resolved
