from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List

from orchestrator.app.llm_provider import LLMProvider

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


async def generate_summary_with_llm(
    provider: LLMProvider,
    tenant_id: str,
    community_id: str,
    member_names: List[str],
    member_types: List[str],
) -> str:
    fallback = generate_summary(member_names, member_types)
    prompt = _build_community_summary_prompt(
        tenant_id=tenant_id,
        community_id=community_id,
        member_names=member_names,
        member_types=member_types,
    )
    try:
        response = await provider.ainvoke(prompt)
        text = str(response).strip()
        return text or fallback
    except Exception:
        logger.warning(
            "LLM community summary failed for tenant=%s community=%s; "
            "using deterministic fallback",
            tenant_id,
            community_id,
            exc_info=True,
        )
        return fallback


def tenant_scoped_community_detection_stub(
    records: List[Dict[str, Any]],
    tenant_id: str,
) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for record in records:
        if str(record.get("tenant_id", "")) != tenant_id:
            continue
        cid = str(record.get("community_id", "unassigned"))
        grouped.setdefault(cid, []).append(record)
    return grouped


def _build_community_summary_prompt(
    tenant_id: str,
    community_id: str,
    member_names: List[str],
    member_types: List[str],
) -> str:
    top_members = ", ".join(member_names[:20])
    type_counts: Dict[str, int] = {}
    for member_type in member_types:
        type_counts[member_type] = type_counts.get(member_type, 0) + 1
    type_summary = ", ".join(
        f"{count}x {member_type}" for member_type, count in sorted(type_counts.items())
    )
    return (
        "You are summarizing a software architecture graph community.\n"
        "Produce 3-5 concise sentences covering:\n"
        "1) architectural role,\n"
        "2) likely dependencies and dependents,\n"
        "3) blast-radius impact if this community fails.\n"
        f"Tenant: {tenant_id}\n"
        f"Community: {community_id}\n"
        f"Member types: {type_summary}\n"
        f"Members: {top_members}\n"
        "Do not invent external systems. Stay grounded in provided members."
    )
