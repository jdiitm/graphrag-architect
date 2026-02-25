from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from orchestrator.app.extraction_models import ServiceNode

DEFAULT_MAX_KNOWN = 100_000


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


def normalize_name(name: str) -> str:
    return name.strip().lower()


@dataclass
class ResolutionResult:
    resolved_id: str
    is_new: bool
    resolved_from: Optional[str] = None


class EntityResolver:
    def __init__(
        self,
        threshold: float = 0.85,
        max_known: int = DEFAULT_MAX_KNOWN,
        **_kwargs: Any,
    ) -> None:
        self._threshold = threshold
        self._max_known = max_known
        self._known: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self._aliases: Dict[str, str] = {}

    @property
    def known_entities(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._known)

    def register_alias(self, alias: str, canonical: str) -> None:
        self._aliases[alias] = canonical

    def _canonical_name(self, name: str) -> str:
        return self._aliases.get(name, name)

    def _evict_if_needed(self) -> None:
        while len(self._known) > self._max_known:
            self._known.popitem(last=False)

    def resolve(
        self,
        name: str,
        repository: str = "",
        namespace: str = "",
        attributes: Optional[Dict[str, Any]] = None,
    ) -> ResolutionResult:
        canonical = self._canonical_name(name)
        entity_id = resolve_entity_id(canonical, repository, namespace)
        attrs = attributes or {}

        if entity_id in self._known:
            self._known.move_to_end(entity_id)
            return ResolutionResult(
                resolved_id=entity_id,
                is_new=False,
                resolved_from=entity_id,
            )

        if canonical != name:
            canonical_id = resolve_entity_id(canonical, repository, namespace)
            if canonical_id in self._known:
                self._known.move_to_end(canonical_id)
                self._known[entity_id] = dict(attrs)
                self._evict_if_needed()
                return ResolutionResult(
                    resolved_id=canonical_id,
                    is_new=False,
                    resolved_from=canonical_id,
                )

        self._known[entity_id] = dict(attrs)
        self._evict_if_needed()
        return ResolutionResult(resolved_id=entity_id, is_new=True)

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
