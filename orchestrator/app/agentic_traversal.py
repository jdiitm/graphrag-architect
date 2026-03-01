from __future__ import annotations

import asyncio
import enum
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from neo4j import AsyncDriver, AsyncManagedTransaction
from neo4j.exceptions import ClientError, Neo4jError

from orchestrator.app.context_manager import (
    TokenBudget,
    estimate_tokens,
    truncate_context,
    truncate_context_topology,
)
from orchestrator.app.tenant_security import (
    TenantSecurityProvider,
    build_traversal_batched_neighbor,
    build_traversal_batched_supernode_neighbor,
    build_traversal_neighbor_discovery,
    build_traversal_one_hop,
    build_traversal_sampled_neighbor,
)

logger = logging.getLogger(__name__)

MAX_HOPS = 5
MAX_VISITED = 50
MAX_NODE_DEGREE = 500
COLLECTION_MULTIPLIER = 2.0

_DEFAULT_DEGREE_THRESHOLD = 200
_DEFAULT_APOC_DEGREE_THRESHOLD = 1000


class TraversalStrategy(enum.Enum):
    BOUNDED_CYPHER = "bounded_cypher"
    BATCHED_BFS = "batched_bfs"
    ADAPTIVE = "adaptive"
    APOC = "apoc"


@dataclass(frozen=True)
class TraversalConfig:
    strategy: TraversalStrategy = TraversalStrategy.ADAPTIVE
    degree_threshold: int = _DEFAULT_DEGREE_THRESHOLD
    apoc_degree_threshold: int = _DEFAULT_APOC_DEGREE_THRESHOLD
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
            apoc_degree_threshold=int(
                os.environ.get(
                    "TRAVERSAL_APOC_DEGREE_THRESHOLD",
                    str(_DEFAULT_APOC_DEGREE_THRESHOLD),
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

_BATCH_DEGREE_CHECK_QUERY = (
    "UNWIND $source_ids AS sid "
    "MATCH (n {id: sid, tenant_id: $tenant_id}) "
    "RETURN n.id AS node_id, size((n)--()) AS degree"
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

_BATCHED_NEIGHBOR_TEMPLATE = build_traversal_batched_neighbor()

_NEIGHBOR_DISCOVERY_NO_ACL = (
    "MATCH (source {id: $source_id, tenant_id: $tenant_id})"
    "-[r]->(target) "
    "WHERE target.tenant_id = $tenant_id "
    "AND r.tombstoned_at IS NULL "
    "RETURN target.id AS target_id, target.name AS target_name, "
    "type(r) AS rel_type, labels(target)[0] AS target_label "
    "ORDER BY coalesce(target.pagerank, 0) DESC, "
    "coalesce(target.degree, 0) DESC, target.id "
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

_APOC_NODES_QUERY = (
    "MATCH (start {id: $source_id, tenant_id: $tenant_id}) "
    "CALL apoc.path.expandConfig(start, {"
    "  maxLevel: $max_hops, limit: $max_visited, bfs: true,"
    "  filterStartNode: false, uniqueness: 'NODE_GLOBAL'"
    "}) YIELD path "
    "UNWIND nodes(path) AS n "
    "WHERE n.tenant_id = $tenant_id "
    "AND ($is_admin OR n.team_owner = $acl_team "
    "OR ANY(ns IN n.namespace_acl WHERE ns IN $acl_namespaces)) "
    "RETURN DISTINCT n.id AS node_id, labels(n) AS labels"
)

_APOC_EDGES_QUERY = (
    "MATCH (start {id: $source_id, tenant_id: $tenant_id}) "
    "CALL apoc.path.expandConfig(start, {"
    "  maxLevel: $max_hops, limit: $max_visited, bfs: true,"
    "  filterStartNode: false, uniqueness: 'NODE_GLOBAL'"
    "}) YIELD path "
    "UNWIND relationships(path) AS r "
    "WHERE r.tombstoned_at IS NULL "
    "RETURN DISTINCT startNode(r).id AS src, endNode(r).id AS tgt, "
    "type(r) AS rel_type"
)


def build_one_hop_cypher(rel_type: str) -> str:
    return build_traversal_one_hop(rel_type)


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
    collection_multiplier: float = COLLECTION_MULTIPLIER

    @property
    def should_continue(self) -> bool:
        if self.remaining_hops <= 0:
            return False
        if len(self.visited_nodes) >= self.max_visited:
            return False
        if not self.frontier:
            return False
        collection_ceiling = int(
            self.token_budget.max_context_tokens * self.collection_multiplier
        )
        if self.current_tokens >= collection_ceiling:
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
        collection_multiplier: float = COLLECTION_MULTIPLIER,
    ) -> None:
        self._max_hops = min(max_hops, MAX_HOPS)
        self._max_visited = min(max_visited, MAX_VISITED)
        self._token_budget = token_budget or TokenBudget()
        self._beam_width = beam_width
        self._frontier_scores: Dict[str, float] = {}
        self._collection_multiplier = collection_multiplier

    def create_state(self, start_node_id: str) -> TraversalState:
        return TraversalState(
            frontier=[start_node_id],
            remaining_hops=self._max_hops,
            max_visited=self._max_visited,
            token_budget=self._token_budget,
            collection_multiplier=self._collection_multiplier,
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
        normalized = _normalize_for_topology(state.accumulated_context)
        return truncate_context_topology(normalized, state.token_budget)


def _normalize_for_topology(
    candidates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for item in candidates:
        entry = dict(item)
        if "source" not in entry and "source_id" in entry:
            entry["source"] = entry["source_id"]
        if "target" not in entry and "target_id" in entry:
            entry["target"] = entry["target_id"]
        if "score" not in entry and "pagerank" in entry:
            entry["score"] = float(entry["pagerank"])
        normalized.append(entry)
    return normalized


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
    provider = TenantSecurityProvider()
    degree_params = {"source_id": source_id, "tenant_id": tenant_id}

    async def _degree_tx(tx: AsyncManagedTransaction) -> List[Dict[str, Any]]:
        result = await tx.run(_DEGREE_CHECK_QUERY, **degree_params)
        return await result.data()

    enforce_acl = not skip_acl
    if skip_acl:
        neighbor_query = _NEIGHBOR_DISCOVERY_NO_ACL
        sampled_query = _SAMPLED_NEIGHBOR_NO_ACL
    else:
        neighbor_query = build_traversal_neighbor_discovery()
        sampled_query = build_traversal_sampled_neighbor()

    validation_params: Dict[str, Any] = {
        "source_id": source_id,
        "tenant_id": tenant_id,
        "limit": limit,
        "sample_size": sample_size,
        **acl_params,
    }
    provider.validate_query(
        neighbor_query, validation_params, require_acl=enforce_acl,
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
                    sample_size, timeout, template=sampled_query,
                )

        params: Dict[str, Any] = {
            "source_id": source_id,
            "tenant_id": tenant_id,
            "limit": limit,
            **acl_params,
        }

        async def _tx(tx: AsyncManagedTransaction) -> List[Dict[str, Any]]:
            result = await tx.run(neighbor_query, **params)
            return await result.data()

        return await session.execute_read(_tx, timeout=timeout)


async def execute_batched_hop(
    driver: AsyncDriver,
    source_ids: List[str],
    tenant_id: str,
    acl_params: Dict[str, Any],
    timeout: float = 30.0,
    limit: int = 200,
    degree_threshold: int = MAX_NODE_DEGREE,
    per_source_limit: int = 50,
) -> List[Dict[str, Any]]:
    if not source_ids:
        return []

    degree_map = await batch_check_degrees(
        driver, source_ids, tenant_id, timeout=timeout,
    )

    normal_frontier = [
        sid for sid in source_ids
        if degree_map.get(sid, 0) <= degree_threshold
    ]
    super_nodes = [
        sid for sid in source_ids
        if degree_map.get(sid, 0) > degree_threshold
    ]

    all_results: List[Dict[str, Any]] = []

    if normal_frontier:
        provider = TenantSecurityProvider()
        batched_query = build_traversal_batched_neighbor()

        params = {
            "frontier_ids": normal_frontier,
            "tenant_id": tenant_id,
            "limit": limit,
            "per_source_limit": per_source_limit,
            **acl_params,
        }
        provider.validate_query(batched_query, params)

        async def _tx(tx: AsyncManagedTransaction) -> List[Dict[str, Any]]:
            result = await tx.run(batched_query, **params)
            return await result.data()

        async with driver.session() as session:
            all_results.extend(
                await session.execute_read(_tx, timeout=timeout)
            )

    if super_nodes:
        logger.warning(
            "Batching %d super-node(s) into single UNWIND expansion: %s",
            len(super_nodes),
            super_nodes,
        )
        supernode_results = await _batched_supernode_expansion(
            driver=driver,
            source_ids=super_nodes,
            tenant_id=tenant_id,
            acl_params=acl_params,
            timeout=timeout,
            sample_size=per_source_limit,
        )
        all_results.extend(supernode_results)

    return all_results


async def batch_check_degrees(
    driver: AsyncDriver,
    source_ids: List[str],
    tenant_id: str,
    timeout: float = 30.0,
) -> Dict[str, int]:
    if not source_ids:
        return {}

    params = {"source_ids": source_ids, "tenant_id": tenant_id}

    async def _tx(tx: AsyncManagedTransaction) -> List[Dict[str, Any]]:
        result = await tx.run(_BATCH_DEGREE_CHECK_QUERY, **params)
        return await result.data()

    async with driver.session() as session:
        records = await session.execute_read(_tx, timeout=timeout)

    return {
        row["node_id"]: int(row["degree"])
        for row in records
        if "node_id" in row and "degree" in row
    }


async def _batched_supernode_expansion(
    driver: AsyncDriver,
    source_ids: List[str],
    tenant_id: str,
    acl_params: Dict[str, Any],
    timeout: float = 30.0,
    sample_size: int = 50,
) -> List[Dict[str, Any]]:
    if not source_ids:
        return []

    provider = TenantSecurityProvider()
    query = build_traversal_batched_supernode_neighbor()

    params: Dict[str, Any] = {
        "source_ids": source_ids,
        "tenant_id": tenant_id,
        "sample_size": sample_size,
        **acl_params,
    }
    provider.validate_query(query, params)

    async def _tx(tx: AsyncManagedTransaction) -> List[Dict[str, Any]]:
        result = await tx.run(query, **params)
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


async def apoc_path_expansion(
    tx: AsyncManagedTransaction,
    source_id: str,
    tenant_id: str,
    acl_params: Dict[str, Any],
    max_hops: int = 3,
    max_visited: int = 500,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "source_id": source_id,
        "tenant_id": tenant_id,
        "max_hops": max_hops,
        "max_visited": max_visited,
        **acl_params,
    }

    node_result = await tx.run(_APOC_NODES_QUERY, **params)
    node_records = await node_result.data()
    nodes: Dict[str, Any] = {r["node_id"]: r["labels"] for r in node_records}

    edge_result = await tx.run(_APOC_EDGES_QUERY, **params)
    edge_records = await edge_result.data()
    valid_ids = set(nodes.keys())
    edges = list(dict.fromkeys(
        (r["src"], r["tgt"], r["rel_type"])
        for r in edge_records
        if r["src"] in valid_ids and r["tgt"] in valid_ids
    ))

    return {"nodes": nodes, "edges": edges}


def _format_apoc_for_traversal(
    raw: Dict[str, Any],
) -> List[Dict[str, Any]]:
    formatted: List[Dict[str, Any]] = []
    nodes_map = raw.get("nodes", {})
    for src, tgt, rel_type in raw.get("edges", []):
        target_labels = nodes_map.get(tgt, [])
        formatted.append({
            "source_id": src,
            "target_id": tgt,
            "rel_type": rel_type,
            "target_label": target_labels[0] if target_labels else "",
        })
    return formatted


async def _try_apoc_expansion(
    driver: AsyncDriver,
    start_node_id: str,
    tenant_id: str,
    acl_params: Dict[str, Any],
    max_hops: int,
    max_visited: int,
    token_budget: TokenBudget,
) -> List[Dict[str, Any]]:
    async def _tx(tx: AsyncManagedTransaction) -> Dict[str, Any]:
        return await apoc_path_expansion(
            tx, start_node_id, tenant_id, acl_params,
            max_hops, max_visited,
        )

    async with driver.session() as session:
        raw = await session.execute_read(_tx)
    formatted = _format_apoc_for_traversal(raw)
    return truncate_context(formatted, token_budget)


async def _resolve_adaptive(
    driver: AsyncDriver,
    start_node_id: str,
    tenant_id: str,
    acl_params: Dict[str, Any],
    max_hops: int,
    timeout: float,
    token_budget: TokenBudget,
    max_visited: int,
    beam_width: int,
    degree_threshold: int,
    degree_hint: Optional[int],
    apoc_degree_threshold: int = _DEFAULT_APOC_DEGREE_THRESHOLD,
) -> List[Dict[str, Any]]:
    if degree_hint is not None:
        if degree_hint >= apoc_degree_threshold:
            try:
                return await _try_apoc_expansion(
                    driver=driver,
                    start_node_id=start_node_id,
                    tenant_id=tenant_id,
                    acl_params=acl_params,
                    max_hops=max_hops,
                    max_visited=max_visited,
                    token_budget=token_budget,
                )
            except ClientError:
                logger.warning(
                    "APOC expansion unavailable for high-degree node %s, "
                    "falling back to batched BFS",
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
                    token_budget=token_budget,
                    max_visited=max_visited,
                    beam_width=beam_width,
                )
        if degree_hint >= degree_threshold:
            return await _batched_bfs(
                driver=driver,
                start_node_id=start_node_id,
                tenant_id=tenant_id,
                acl_params=acl_params,
                max_hops=max_hops,
                timeout=timeout,
                token_budget=token_budget,
                max_visited=max_visited,
                beam_width=beam_width,
            )
        return await _run_bounded_with_fallback(
            driver=driver,
            start_node_id=start_node_id,
            tenant_id=tenant_id,
            acl_params=acl_params,
            max_hops=max_hops,
            timeout=timeout,
            effective_budget=token_budget,
            max_visited=max_visited,
            beam_width=beam_width,
        )
    try:
        return await _try_apoc_expansion(
            driver=driver,
            start_node_id=start_node_id,
            tenant_id=tenant_id,
            acl_params=acl_params,
            max_hops=max_hops,
            max_visited=max_visited,
            token_budget=token_budget,
        )
    except ClientError:
        logger.warning(
            "APOC expansion unavailable for node %s, "
            "falling back to batched BFS",
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
            token_budget=token_budget,
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
    degree_hint: Optional[int] = None,
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

    if config.strategy == TraversalStrategy.APOC:
        return await _try_apoc_expansion(
            driver=driver,
            start_node_id=start_node_id,
            tenant_id=tenant_id,
            acl_params=acl_params,
            max_hops=effective_hops,
            max_visited=effective_max_visited,
            token_budget=effective_budget,
        )

    if config.strategy == TraversalStrategy.ADAPTIVE:
        return await _resolve_adaptive(
            driver=driver,
            start_node_id=start_node_id,
            tenant_id=tenant_id,
            acl_params=acl_params,
            max_hops=effective_hops,
            timeout=effective_timeout,
            token_budget=effective_budget,
            max_visited=effective_max_visited,
            beam_width=config.beam_width,
            degree_threshold=config.degree_threshold,
            degree_hint=degree_hint,
            apoc_degree_threshold=config.apoc_degree_threshold,
        )

    raise ValueError(f"Unknown traversal strategy: {config.strategy}")
