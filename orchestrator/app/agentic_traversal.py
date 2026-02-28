from __future__ import annotations

import asyncio
import enum
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from neo4j import AsyncDriver, AsyncManagedTransaction
from neo4j.exceptions import Neo4jError

from orchestrator.app.context_manager import TokenBudget, estimate_tokens, truncate_context
from orchestrator.app.query_templates import ALLOWED_RELATIONSHIP_TYPES

logger = logging.getLogger(__name__)

MAX_HOPS = 5
MAX_VISITED = 50
MAX_NODE_DEGREE = 500

_DEFAULT_DEGREE_THRESHOLD = 200


class TraversalStrategy(enum.Enum):
    BOUNDED_CYPHER = "bounded_cypher"
    BATCHED_BFS = "batched_bfs"
    ADAPTIVE = "adaptive"


@dataclass(frozen=True)
class TraversalConfig:
    strategy: TraversalStrategy = TraversalStrategy.ADAPTIVE
    degree_threshold: int = _DEFAULT_DEGREE_THRESHOLD
    max_hops: int = MAX_HOPS
    max_visited: int = MAX_VISITED
    timeout: float = 30.0
    beam_width: int = 50

    @classmethod
    def from_env(cls) -> TraversalConfig:
        raw_strategy = os.environ.get("TRAVERSAL_STRATEGY", "adaptive").lower()
        strategy = TraversalStrategy(raw_strategy)
        return cls(
            strategy=strategy,
            degree_threshold=int(
                os.environ.get(
                    "TRAVERSAL_DEGREE_THRESHOLD",
                    str(_DEFAULT_DEGREE_THRESHOLD),
                )
            ),
            max_hops=int(os.environ.get("TRAVERSAL_MAX_HOPS", str(MAX_HOPS))),
            max_visited=int(
                os.environ.get("TRAVERSAL_MAX_VISITED", str(MAX_VISITED))
            ),
            timeout=float(os.environ.get("TRAVERSAL_TIMEOUT", "30.0")),
            beam_width=int(os.environ.get("TRAVERSAL_BEAM_WIDTH", "50")),
        )


_DEGREE_CHECK_QUERY = (
    "MATCH (n {id: $source_id, tenant_id: $tenant_id}) "
    "RETURN size((n)--()) AS degree"
)

_ONE_HOP_TEMPLATE = (
    "MATCH (source {{id: $source_id, tenant_id: $tenant_id}})"
    "-[r:{rel_type}]->(target) "
    "WHERE target.tenant_id = $tenant_id "
    "AND r.tombstoned_at IS NULL "
    "AND ($is_admin OR target.team_owner = $acl_team "
    "OR ANY(ns IN target.namespace_acl WHERE ns IN $acl_namespaces)) "
    "RETURN target {{.*}} AS result, type(r) AS rel_type "
    "LIMIT $limit"
)

_NEIGHBOR_DISCOVERY_TEMPLATE = (
    "MATCH (source {{id: $source_id, tenant_id: $tenant_id}})"
    "-[r]->(target) "
    "WHERE target.tenant_id = $tenant_id "
    "AND r.tombstoned_at IS NULL "
    "AND ($is_admin OR target.team_owner = $acl_team "
    "OR ANY(ns IN target.namespace_acl WHERE ns IN $acl_namespaces)) "
    "RETURN target.id AS target_id, target.name AS target_name, "
    "type(r) AS rel_type, labels(target)[0] AS target_label "
    "LIMIT $limit"
)

_SAMPLED_NEIGHBOR_TEMPLATE = (
    "MATCH (source {id: $source_id, tenant_id: $tenant_id})"
    "-[r]->(target) "
    "WHERE target.tenant_id = $tenant_id AND r.tombstoned_at IS NULL "
    "AND ($is_admin OR target.team_owner = $acl_team "
    "OR ANY(ns IN target.namespace_acl WHERE ns IN $acl_namespaces)) "
    "RETURN target.id AS target_id, target.name AS target_name, "
    "type(r) AS rel_type, labels(target)[0] AS target_label "
    "ORDER BY coalesce(target.pagerank, 0) DESC, "
    "coalesce(target.degree, 0) DESC, target.id "
    "LIMIT $sample_size"
)

_BATCHED_NEIGHBOR_TEMPLATE = (
    "UNWIND $frontier_ids AS fid "
    "MATCH (source {id: fid, tenant_id: $tenant_id})"
    "-[r]->(target) "
    "WHERE target.tenant_id = $tenant_id "
    "AND r.tombstoned_at IS NULL "
    "AND ($is_admin OR target.team_owner = $acl_team "
    "OR ANY(ns IN target.namespace_acl WHERE ns IN $acl_namespaces)) "
    "RETURN source.id AS source_id, target.id AS target_id, "
    "target.name AS target_name, type(r) AS rel_type, "
    "labels(target)[0] AS target_label, "
    "coalesce(target.pagerank, 0) AS pagerank, "
    "coalesce(target.degree, 0) AS degree "
    "LIMIT $limit"
)

_NEIGHBOR_DISCOVERY_NO_ACL = (
    "MATCH (source {id: $source_id, tenant_id: $tenant_id})"
    "-[r]->(target) "
    "WHERE target.tenant_id = $tenant_id "
    "AND r.tombstoned_at IS NULL "
    "RETURN target.id AS target_id, target.name AS target_name, "
    "type(r) AS rel_type, labels(target)[0] AS target_label "
    "LIMIT $limit"
)

_SAMPLED_NEIGHBOR_NO_ACL = (
    "MATCH (source {id: $source_id, tenant_id: $tenant_id})"
    "-[r]->(target) "
    "WHERE target.tenant_id = $tenant_id AND r.tombstoned_at IS NULL "
    "RETURN target.id AS target_id, target.name AS target_name, "
    "type(r) AS rel_type, labels(target)[0] AS target_label "
    "ORDER BY coalesce(target.pagerank, 0) DESC, "
    "coalesce(target.degree, 0) DESC, target.id "
    "LIMIT $sample_size"
)

_SEMANTIC_PRUNED_NEIGHBOR_TEMPLATE = (
    "MATCH (source {id: $source_id, tenant_id: $tenant_id})"
    "-[r]->(target) "
    "WHERE target.tenant_id = $tenant_id AND r.tombstoned_at IS NULL "
    "AND ($is_admin OR target.team_owner = $acl_team "
    "OR ANY(ns IN target.namespace_acl WHERE ns IN $acl_namespaces)) "
    "AND (CASE WHEN $query_embedding IS NOT NULL "
    "THEN vector.similarity.cosine(target.embedding, $query_embedding) > $sim_threshold "
    "ELSE true END) "
    "RETURN target.id AS target_id, target.name AS target_name, "
    "type(r) AS rel_type, labels(target)[0] AS target_label "
    "ORDER BY coalesce(target.pagerank, 0) DESC, "
    "coalesce(target.degree, 0) DESC, target.id "
    "LIMIT $sample_size"
)

_SEMANTIC_PRUNED_NEIGHBOR_NO_ACL = (
    "MATCH (source {id: $source_id, tenant_id: $tenant_id})"
    "-[r]->(target) "
    "WHERE target.tenant_id = $tenant_id AND r.tombstoned_at IS NULL "
    "AND (CASE WHEN $query_embedding IS NOT NULL "
    "THEN vector.similarity.cosine(target.embedding, $query_embedding) > $sim_threshold "
    "ELSE true END) "
    "RETURN target.id AS target_id, target.name AS target_name, "
    "type(r) AS rel_type, labels(target)[0] AS target_label "
    "ORDER BY coalesce(target.pagerank, 0) DESC, "
    "coalesce(target.degree, 0) DESC, target.id "
    "LIMIT $sample_size"
)

_BOUNDED_PATH_TEMPLATE = (
    "MATCH path = (source {{id: $start_id, tenant_id: $tenant_id}})"
    "-[*1..{max_hops}]->(target) "
    "WHERE all(n IN nodes(path) WHERE n.tenant_id = $tenant_id) "
    "AND all(r IN relationships(path) WHERE r.tombstoned_at IS NULL) "
    "AND all(n IN nodes(path) WHERE $is_admin OR n.team_owner = $acl_team "
    "OR ANY(ns IN n.namespace_acl WHERE ns IN $acl_namespaces)) "
    "WITH DISTINCT target "
    "RETURN target.id AS target_id, target.name AS target_name, "
    "labels(target)[0] AS target_label "
    "LIMIT $max_nodes"
)


def build_one_hop_cypher(rel_type: str) -> str:
    if rel_type not in ALLOWED_RELATIONSHIP_TYPES:
        raise ValueError(f"Disallowed relationship type: {rel_type}")
    return _ONE_HOP_TEMPLATE.format(rel_type=rel_type)


def compute_node_score(result: Dict[str, Any]) -> float:
    pagerank = float(result.get("pagerank", 0.0))
    degree = float(result.get("degree", 0))
    return pagerank + degree / 1000.0


@dataclass
class TraversalState:
    visited_nodes: Set[str] = field(default_factory=set)
    frontier: List[str] = field(default_factory=list)
    accumulated_context: List[Dict[str, Any]] = field(default_factory=list)
    remaining_hops: int = MAX_HOPS
    max_visited: int = MAX_VISITED
    token_budget: TokenBudget = field(default_factory=TokenBudget)
    current_tokens: int = 0

    @property
    def should_continue(self) -> bool:
        if self.remaining_hops <= 0:
            return False
        if len(self.visited_nodes) >= self.max_visited:
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
        beam_width: int = 50,
    ) -> None:
        self._max_hops = min(max_hops, MAX_HOPS)
        self._max_visited = min(max_visited, MAX_VISITED)
        self._token_budget = token_budget or TokenBudget()
        self._beam_width = beam_width
        self._frontier_scores: Dict[str, float] = {}

    def create_state(self, start_node_id: str) -> TraversalState:
        return TraversalState(
            frontier=[start_node_id],
            remaining_hops=self._max_hops,
            max_visited=self._max_visited,
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

        score_map: Dict[str, float] = {}
        for result in step.results:
            tid = result.get("target_id")
            if tid:
                score_map[tid] = compute_node_score(result)

        for nid in step.new_frontier:
            if nid not in state.visited_nodes and nid not in state.frontier:
                state.frontier.append(nid)
                if nid in score_map:
                    self._frontier_scores[nid] = score_map[nid]

        state.frontier = [
            nid for nid in state.frontier if nid not in state.visited_nodes
        ]

        state.frontier.sort(
            key=lambda node_id: self._frontier_scores.get(node_id, 0.0),
            reverse=True,
        )
        if len(state.frontier) > self._beam_width:
            discarded = state.frontier[self._beam_width:]
            for nid in discarded:
                self._frontier_scores.pop(nid, None)
            state.frontier = state.frontier[:self._beam_width]

    def get_context(self, state: TraversalState) -> List[Dict[str, Any]]:
        return truncate_context(
            state.accumulated_context,
            state.token_budget,
        )


async def bounded_path_expansion(
    driver: AsyncDriver,
    start_id: str,
    tenant_id: str,
    acl_params: Dict[str, Any],
    max_hops: int = MAX_HOPS,
    max_nodes: int = MAX_VISITED,
    timeout: float = 30.0,
) -> List[Dict[str, Any]]:
    if not 1 <= max_hops <= MAX_HOPS:
        raise ValueError(
            f"max_hops must be between 1 and {MAX_HOPS}, got {max_hops}"
        )

    cypher = _BOUNDED_PATH_TEMPLATE.format(max_hops=max_hops)
    params = {
        "start_id": start_id,
        "tenant_id": tenant_id,
        "max_nodes": max_nodes,
        **acl_params,
    }

    async def _tx(tx: AsyncManagedTransaction) -> List[Dict[str, Any]]:
        result = await tx.run(cypher, **params)
        return await result.data()

    async with driver.session() as session:
        return await session.execute_read(_tx, timeout=timeout)


async def _run_sampled_query(
    session: Any,
    source_id: str,
    tenant_id: str,
    acl_params: Dict[str, Any],
    sample_size: int,
    timeout: float,
    template: str = "",
    query_embedding: Optional[List[float]] = None,
    similarity_threshold: float = 0.5,
) -> List[Dict[str, Any]]:
    effective_template = template or _SAMPLED_NEIGHBOR_TEMPLATE
    params: Dict[str, Any] = {
        "source_id": source_id,
        "tenant_id": tenant_id,
        "sample_size": sample_size,
        **acl_params,
    }
    if query_embedding is not None:
        params["query_embedding"] = query_embedding
        params["sim_threshold"] = similarity_threshold

    async def _sample_tx(tx: AsyncManagedTransaction) -> List[Dict[str, Any]]:
        result = await tx.run(effective_template, **params)
        return await result.data()

    return await session.execute_read(_sample_tx, timeout=timeout)


async def execute_hop(
    driver: AsyncDriver,
    source_id: str,
    tenant_id: str,
    acl_params: Dict[str, Any],
    timeout: float = 30.0,
    limit: int = 50,
    max_degree: int = MAX_NODE_DEGREE,
    sample_size: int = 50,
    skip_acl: bool = False,
    query_embedding: Optional[List[float]] = None,
    similarity_threshold: float = 0.5,
) -> List[Dict[str, Any]]:
    degree_params = {"source_id": source_id, "tenant_id": tenant_id}

    async def _degree_tx(tx: AsyncManagedTransaction) -> List[Dict[str, Any]]:
        result = await tx.run(_DEGREE_CHECK_QUERY, **degree_params)
        return await result.data()

    neighbor_template = (
        _NEIGHBOR_DISCOVERY_NO_ACL if skip_acl
        else _NEIGHBOR_DISCOVERY_TEMPLATE
    )
    sampled_template = (
        _SAMPLED_NEIGHBOR_NO_ACL if skip_acl
        else _SAMPLED_NEIGHBOR_TEMPLATE
    )

    async with driver.session() as session:
        degree_result = await session.execute_read(_degree_tx, timeout=timeout)
        if degree_result:
            degree = degree_result[0].get("degree", 0)
            if degree > max_degree:
                logger.warning(
                    "Sampling super-node %s with degree %d (threshold %d, sample %d)",
                    source_id, degree, max_degree, sample_size,
                )
                if query_embedding is not None:
                    semantic_template = (
                        _SEMANTIC_PRUNED_NEIGHBOR_NO_ACL if skip_acl
                        else _SEMANTIC_PRUNED_NEIGHBOR_TEMPLATE
                    )
                    return await _run_sampled_query(
                        session, source_id, tenant_id, acl_params,
                        sample_size, timeout, template=semantic_template,
                        query_embedding=query_embedding,
                        similarity_threshold=similarity_threshold,
                    )
                return await _run_sampled_query(
                    session, source_id, tenant_id, acl_params,
                    sample_size, timeout, template=sampled_template,
                )

        params: Dict[str, Any] = {
            "source_id": source_id,
            "tenant_id": tenant_id,
            "limit": limit,
            **acl_params,
        }

        async def _tx(tx: AsyncManagedTransaction) -> List[Dict[str, Any]]:
            result = await tx.run(neighbor_template, **params)
            return await result.data()

        return await session.execute_read(_tx, timeout=timeout)


async def execute_batched_hop(
    driver: AsyncDriver,
    source_ids: List[str],
    tenant_id: str,
    acl_params: Dict[str, Any],
    timeout: float = 30.0,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    if not source_ids:
        return []

    params = {
        "frontier_ids": source_ids,
        "tenant_id": tenant_id,
        "limit": limit,
        **acl_params,
    }

    async def _tx(tx: AsyncManagedTransaction) -> List[Dict[str, Any]]:
        result = await tx.run(_BATCHED_NEIGHBOR_TEMPLATE, **params)
        return await result.data()

    async with driver.session() as session:
        return await session.execute_read(_tx, timeout=timeout)


def _drain_frontier(state: TraversalState) -> List[str]:
    batch: List[str] = []
    seen: Set[str] = set()
    while state.frontier:
        candidate = state.frontier.pop(0)
        if candidate not in state.visited_nodes and candidate not in seen:
            batch.append(candidate)
            seen.add(candidate)
    return batch


def _record_batched_results(
    agent: TraversalAgent,
    state: TraversalState,
    frontier_batch: List[str],
    results: List[Dict[str, Any]],
    hop_number: int,
) -> None:
    for node_id in frontier_batch:
        node_results = [r for r in results if r.get("source_id") == node_id]
        new_frontier = [r["target_id"] for r in node_results if "target_id" in r]
        step = TraversalStep(
            node_id=node_id,
            hop_number=hop_number,
            results=node_results,
            new_frontier=new_frontier,
        )
        agent.record_step(state, step)


async def _batched_bfs(
    driver: AsyncDriver,
    start_node_id: str,
    tenant_id: str,
    acl_params: Dict[str, Any],
    max_hops: int,
    timeout: float,
    token_budget: TokenBudget,
    max_visited: int = MAX_VISITED,
    beam_width: int = 50,
) -> List[Dict[str, Any]]:
    agent = TraversalAgent(
        max_hops=max_hops,
        max_visited=max_visited,
        token_budget=token_budget,
        beam_width=beam_width,
    )
    state = agent.create_state(start_node_id)
    hop_number = 0

    while state.should_continue:
        frontier_batch = _drain_frontier(state)
        if not frontier_batch:
            break

        hop_number += 1
        results = await execute_batched_hop(
            driver=driver,
            source_ids=frontier_batch,
            tenant_id=tenant_id,
            acl_params=acl_params,
            timeout=timeout,
        )
        _record_batched_results(agent, state, frontier_batch, results, hop_number)

    return agent.get_context(state)


async def _probe_start_degree(
    driver: AsyncDriver,
    start_id: str,
    tenant_id: str,
) -> int:
    params = {"source_id": start_id, "tenant_id": tenant_id}

    async def _tx(tx: AsyncManagedTransaction) -> List[Dict[str, Any]]:
        result = await tx.run(_DEGREE_CHECK_QUERY, **params)
        return await result.data()

    try:
        async with driver.session() as session:
            records = await session.execute_read(_tx, timeout=10.0)
            if records:
                return int(records[0].get("degree", 0))
            return 0
    except (Neo4jError, asyncio.TimeoutError, OSError):
        logger.warning(
            "Degree probe failed for node %s, defaulting to 0",
            start_id,
            exc_info=True,
        )
        return 0


async def _run_bounded_with_fallback(
    driver: AsyncDriver,
    start_node_id: str,
    tenant_id: str,
    acl_params: Dict[str, Any],
    max_hops: int,
    timeout: float,
    effective_budget: TokenBudget,
    max_visited: int = MAX_VISITED,
    beam_width: int = 50,
) -> List[Dict[str, Any]]:
    try:
        raw_results = await bounded_path_expansion(
            driver=driver,
            start_id=start_node_id,
            tenant_id=tenant_id,
            acl_params=acl_params,
            max_hops=max_hops,
            max_nodes=max_visited,
            timeout=timeout,
        )
        return truncate_context(raw_results, effective_budget)
    except (Neo4jError, asyncio.TimeoutError, OSError):
        logger.warning(
            "Bounded path expansion failed for node %s, falling back to batched BFS",
            start_node_id,
            exc_info=True,
        )

    return await _batched_bfs(
        driver=driver,
        start_node_id=start_node_id,
        tenant_id=tenant_id,
        acl_params=acl_params,
        max_hops=max_hops,
        timeout=timeout,
        token_budget=effective_budget,
        max_visited=max_visited,
        beam_width=beam_width,
    )


async def run_traversal(
    driver: AsyncDriver,
    start_node_id: str,
    tenant_id: str,
    acl_params: Dict[str, Any],
    max_hops: int = MAX_HOPS,
    timeout: float = 30.0,
    token_budget: Optional[TokenBudget] = None,
    config: Optional[TraversalConfig] = None,
) -> List[Dict[str, Any]]:
    effective_budget = token_budget or TokenBudget()

    if config is None:
        return await _run_bounded_with_fallback(
            driver=driver,
            start_node_id=start_node_id,
            tenant_id=tenant_id,
            acl_params=acl_params,
            max_hops=max_hops,
            timeout=timeout,
            effective_budget=effective_budget,
        )

    effective_hops = min(max_hops, config.max_hops)
    effective_timeout = min(timeout, config.timeout)
    effective_max_visited = config.max_visited

    if config.strategy == TraversalStrategy.BATCHED_BFS:
        return await _batched_bfs(
            driver=driver,
            start_node_id=start_node_id,
            tenant_id=tenant_id,
            acl_params=acl_params,
            max_hops=effective_hops,
            timeout=effective_timeout,
            token_budget=effective_budget,
            max_visited=effective_max_visited,
            beam_width=config.beam_width,
        )

    if config.strategy == TraversalStrategy.BOUNDED_CYPHER:
        return await _run_bounded_with_fallback(
            driver=driver,
            start_node_id=start_node_id,
            tenant_id=tenant_id,
            acl_params=acl_params,
            max_hops=effective_hops,
            timeout=effective_timeout,
            effective_budget=effective_budget,
            max_visited=effective_max_visited,
            beam_width=config.beam_width,
        )

    if config.strategy != TraversalStrategy.ADAPTIVE:
        raise ValueError(f"Unknown traversal strategy: {config.strategy}")

    degree = await _probe_start_degree(driver, start_node_id, tenant_id)

    if degree >= config.degree_threshold:
        logger.info(
            "ADAPTIVE: high-degree node %s (degree=%d, threshold=%d), using batched BFS",
            start_node_id,
            degree,
            config.degree_threshold,
        )
        return await _batched_bfs(
            driver=driver,
            start_node_id=start_node_id,
            tenant_id=tenant_id,
            acl_params=acl_params,
            max_hops=effective_hops,
            timeout=effective_timeout,
            token_budget=effective_budget,
            max_visited=effective_max_visited,
            beam_width=config.beam_width,
        )

    logger.info(
        "ADAPTIVE: low-degree node %s (degree=%d, threshold=%d), using bounded Cypher",
        start_node_id,
        degree,
        config.degree_threshold,
    )
    return await _run_bounded_with_fallback(
        driver=driver,
        start_node_id=start_node_id,
        tenant_id=tenant_id,
        acl_params=acl_params,
        max_hops=effective_hops,
        timeout=effective_timeout,
        effective_budget=effective_budget,
        max_visited=effective_max_visited,
        beam_width=config.beam_width,
    )
