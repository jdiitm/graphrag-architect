from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List


@dataclass(frozen=True)
class MacroNode:
    node_id: str
    community_id: str
    tenant_id: str
    member_ids: FrozenSet[str]
    summary_text: str
    member_count: int
    centroid_embedding: List[float] = field(default_factory=list)

    @property
    def neo4j_label(self) -> str:
        return "MacroNode"


def is_macro_node(result: Dict[str, Any]) -> bool:
    return result.get("target_label") == "MacroNode"
