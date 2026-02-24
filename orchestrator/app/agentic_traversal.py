from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from orchestrator.app.context_manager import TokenBudget, estimate_tokens, truncate_context
from orchestrator.app.query_templates import ALLOWED_RELATIONSHIP_TYPES

logger = logging.getLogger(__name__)

MAX_HOPS = 5
MAX_VISITED = 50

_ONE_HOP_TEMPLATE = (
    "MATCH (source {{id: $source_id, tenant_id: $tenant_id}})"
    "-[r:{rel_type}]->(target) "
    "WHERE target.tenant_id = $tenant_id "
    "AND ($is_admin OR target.team_owner = $acl_team "
    "OR ANY(ns IN target.namespace_acl WHERE ns IN $acl_namespaces)) "
    "RETURN target {{.*}} AS result, type(r) AS rel_type "
    "LIMIT $limit"
)

_NEIGHBOR_DISCOVERY_TEMPLATE = (
    "MATCH (source {{id: $source_id, tenant_id: $tenant_id}})"
    "-[r]->(target) "
    "WHERE target.tenant_id = $tenant_id "
    "AND ($is_admin OR target.team_owner = $acl_team "
    "OR ANY(ns IN target.namespace_acl WHERE ns IN $acl_namespaces)) "
    "RETURN target.id AS target_id, target.name AS target_name, "
    "type(r) AS rel_type, labels(target)[0] AS target_label "
    "LIMIT $limit"
)


def build_one_hop_cypher(rel_type: str) -> str:
    if rel_type not in ALLOWED_RELATIONSHIP_TYPES:
        raise ValueError(f"Disallowed relationship type: {rel_type}")
    return _ONE_HOP_TEMPLATE.format(rel_type=rel_type)


@dataclass
class TraversalState:
    visited_nodes: Set[str] = field(default_factory=set)
    frontier: List[str] = field(default_factory=list)
    accumulated_context: List[Dict[str, Any]] = field(default_factory=list)
    remaining_hops: int = MAX_HOPS
    token_budget: TokenBudget = field(default_factory=TokenBudget)
    current_tokens: int = 0

    @property
    def should_continue(self) -> bool:
        if self.remaining_hops <= 0:
            return False
        if len(self.visited_nodes) >= MAX_VISITED:
            return False
        if not self.frontier:
            return False
        if self.current_tokens >= self.token_budget.max_context_tokens:
            return False
        return True


@dataclass
class TraversalStep:
    node_id: str
    hop_number: int
    results: List[Dict[str, Any]]
    new_frontier: List[str]


class TraversalAgent:
    def __init__(
        self,
        max_hops: int = MAX_HOPS,
        max_visited: int = MAX_VISITED,
        token_budget: Optional[TokenBudget] = None,
    ) -> None:
        self._max_hops = min(max_hops, MAX_HOPS)
        self._max_visited = min(max_visited, MAX_VISITED)
        self._token_budget = token_budget or TokenBudget()

    def create_state(self, start_node_id: str) -> TraversalState:
        return TraversalState(
            frontier=[start_node_id],
            remaining_hops=self._max_hops,
            token_budget=self._token_budget,
        )

    def select_next_node(self, state: TraversalState) -> Optional[str]:
        while state.frontier:
            candidate = state.frontier.pop(0)
            if candidate not in state.visited_nodes:
                return candidate
        return None

    def record_step(
        self,
        state: TraversalState,
        step: TraversalStep,
    ) -> None:
        state.visited_nodes.add(step.node_id)
        state.remaining_hops -= 1
        for result in step.results:
            token_count = estimate_tokens(str(result))
            if state.current_tokens + token_count > state.token_budget.max_context_tokens:
                break
            state.accumulated_context.append(result)
            state.current_tokens += token_count
        for nid in step.new_frontier:
            if nid not in state.visited_nodes and len(state.frontier) < self._max_visited:
                state.frontier.append(nid)

    def get_context(self, state: TraversalState) -> List[Dict[str, Any]]:
        return truncate_context(
            state.accumulated_context,
            state.token_budget,
        )
