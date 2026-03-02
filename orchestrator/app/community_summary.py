from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CommunitySummary:
    community_id: str
    tenant_id: str
    member_count: int
    summary_text: str
    member_ids: FrozenSet[str] = field(default_factory=frozenset)


class CommunitySummaryStore:
    def __init__(self) -> None:
        self._store: Dict[str, Dict[str, CommunitySummary]] = {}

    def upsert(self, summary: CommunitySummary) -> None:
        tenant_map = self._store.setdefault(summary.tenant_id, {})
        tenant_map[summary.community_id] = summary

    def get_by_tenant(self, tenant_id: str) -> List[CommunitySummary]:
        tenant_map = self._store.get(tenant_id)
        if tenant_map is None:
            return []
        return list(tenant_map.values())

    def get_by_community_id(
        self, tenant_id: str, community_id: str,
    ) -> CommunitySummary | None:
        tenant_map = self._store.get(tenant_id)
        if tenant_map is None:
            return None
        return tenant_map.get(community_id)


def retrieve_community_summaries(
    store: CommunitySummaryStore,
    tenant_id: str,
) -> List[CommunitySummary]:
    return store.get_by_tenant(tenant_id)


def generate_summary(
    member_names: List[str],
    member_types: List[str],
) -> str:
    type_counts: Dict[str, int] = {}
    for mtype in member_types:
        type_counts[mtype] = type_counts.get(mtype, 0) + 1

    parts: List[str] = []
    for node_type, count in sorted(type_counts.items()):
        parts.append(f"{count} {node_type}(s)")
    type_summary = ", ".join(parts)

    names_str = ", ".join(member_names[:10])
    if len(member_names) > 10:
        names_str += f" and {len(member_names) - 10} more"

    return (
        f"Community of {len(member_names)} nodes ({type_summary}): "
        f"{names_str}."
    )
