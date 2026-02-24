from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


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
    def __init__(self, threshold: float = 0.85) -> None:
        self._threshold = threshold
        self._known: Dict[str, Dict[str, Any]] = {}

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

        self._known[entity_id] = dict(attrs)
        return ResolutionResult(resolved_id=entity_id, is_new=True)
