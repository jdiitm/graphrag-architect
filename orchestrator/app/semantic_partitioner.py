from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Set

from orchestrator.app.graph_embeddings import GraphTopology

logger = logging.getLogger(__name__)

DEFAULT_RESOLUTION = 1.0
DEFAULT_MAX_ITERATIONS = 10


@dataclass(frozen=True)
class Community:
    community_id: str
    members: FrozenSet[str]
    modularity_score: float = 0.0


@dataclass
class PartitionResult:
    communities: List[Community] = field(default_factory=list)
    node_to_community: Dict[str, str] = field(default_factory=dict)
    modularity: float = 0.0

    @property
    def community_count(self) -> int:
        return len(self.communities)


def _compute_modularity(
    topology: GraphTopology,
    partition: Dict[str, int],
    total_edges: int,
) -> float:
    if total_edges == 0:
        return 0.0
    m2 = 2 * total_edges
    degree: Dict[str, int] = {}
    for node in topology.nodes:
        degree[node] = len(topology.neighbors(node))

    q_sum = 0.0
    for node_i in topology.nodes:
        for node_j in topology.neighbors(node_i):
            if partition.get(node_i) != partition.get(node_j):
                continue
            a_ij = 1
            q_sum += a_ij - (degree[node_i] * degree[node_j]) / m2
    return q_sum / m2


def _count_edges(topology: GraphTopology) -> int:
    total = sum(len(topology.neighbors(n)) for n in topology.nodes)
    return total // 2


def _find_best_community(
    node: str,
    topology: GraphTopology,
    partition: Dict[str, int],
    degree: Dict[str, int],
    resolution: float,
    m2: int,
) -> int:
    current_comm = partition[node]
    neighbor_comms: Dict[int, float] = defaultdict(float)
    for nbr in topology.neighbors(node):
        neighbor_comms[partition[nbr]] += 1.0

    best_comm = current_comm
    best_gain = 0.0
    for comm, ki_in in neighbor_comms.items():
        if comm == current_comm:
            continue
        sigma_tot = sum(
            degree[n] for n in topology.nodes if partition[n] == comm
        )
        gain = ki_in - resolution * (sigma_tot * degree[node]) / m2
        if gain > best_gain:
            best_gain = gain
            best_comm = comm
    return best_comm


def _louvain_partition(
    topology: GraphTopology,
    resolution: float = DEFAULT_RESOLUTION,
    max_iterations: int = DEFAULT_MAX_ITERATIONS,
) -> Dict[str, int]:
    if not topology.nodes:
        return {}

    partition: Dict[str, int] = {
        node: i for i, node in enumerate(topology.nodes)
    }
    total_edges = _count_edges(topology)
    if total_edges == 0:
        return partition

    degree: Dict[str, int] = {
        node: len(topology.neighbors(node))
        for node in topology.nodes
    }

    for _ in range(max_iterations):
        moved = False
        for node in topology.nodes:
            best = _find_best_community(
                node, topology, partition, degree,
                resolution, 2 * total_edges,
            )
            if best != partition[node]:
                partition[node] = best
                moved = True
        if not moved:
            break

    comm_ids = sorted(set(partition.values()))
    remap = {old: new for new, old in enumerate(comm_ids)}
    return {node: remap[comm] for node, comm in partition.items()}


class SemanticPartitioner:
    def __init__(
        self,
        resolution: float = DEFAULT_RESOLUTION,
        max_iterations: int = DEFAULT_MAX_ITERATIONS,
    ) -> None:
        self._resolution = resolution
        self._max_iterations = max_iterations

    def partition(
        self, topology: GraphTopology,
    ) -> PartitionResult:
        raw_partition = _louvain_partition(
            topology,
            resolution=self._resolution,
            max_iterations=self._max_iterations,
        )

        community_members: Dict[int, Set[str]] = defaultdict(set)
        for node, comm_id in raw_partition.items():
            community_members[comm_id].add(node)

        total_edges = _count_edges(topology)
        modularity = _compute_modularity(
            topology, raw_partition, total_edges,
        )

        communities: List[Community] = []
        node_to_community: Dict[str, str] = {}
        for comm_id, members in sorted(community_members.items()):
            cid = f"community-{comm_id}"
            communities.append(Community(
                community_id=cid,
                members=frozenset(members),
                modularity_score=modularity,
            ))
            for member in members:
                node_to_community[member] = cid

        return PartitionResult(
            communities=communities,
            node_to_community=node_to_community,
            modularity=modularity,
        )


GDS_NODE_THRESHOLD = 10_000


class GDSPartitioner:
    def __init__(self, driver: Any = None) -> None:
        self._driver = driver
        self._fallback = SemanticPartitioner()

    async def partition_async(
        self,
        graph_name: str,
        node_label: str,
        relationship_type: str = "RELATES_TO",
    ) -> PartitionResult:
        async def _tx(tx: Any) -> list:
            result = await tx.run(
                "CALL gds.louvain.stream($graph_name, {"
                "  nodeLabels: [$node_label],"
                "  relationshipTypes: [$rel_type]"
                "}) YIELD nodeId, communityId "
                "RETURN gds.util.asNode(nodeId).name AS nodeId, "
                "communityId",
                graph_name=graph_name,
                node_label=node_label,
                rel_type=relationship_type,
            )
            return await result.data()

        async with self._driver.session() as session:
            records = await session.execute_read(_tx)

        return self._records_to_result(records)

    def partition_with_fallback(
        self,
        topology: GraphTopology,
        node_threshold: int = GDS_NODE_THRESHOLD,
    ) -> PartitionResult:
        if len(topology.nodes) < node_threshold:
            return self._fallback.partition(topology)
        logger.info(
            "Graph has %d nodes (threshold=%d), "
            "requires async GDS partitioning via partition_async()",
            len(topology.nodes), node_threshold,
        )
        return self._fallback.partition(topology)

    @staticmethod
    def _records_to_result(records: List[dict]) -> PartitionResult:
        if not records:
            return PartitionResult()

        community_members: Dict[int, Set[str]] = defaultdict(set)
        for rec in records:
            community_members[rec["communityId"]].add(rec["nodeId"])

        communities: List[Community] = []
        node_to_community: Dict[str, str] = {}
        for comm_id, members in sorted(community_members.items()):
            cid = f"community-{comm_id}"
            communities.append(Community(
                community_id=cid,
                members=frozenset(members),
            ))
            for member in members:
                node_to_community[member] = cid

        return PartitionResult(
            communities=communities,
            node_to_community=node_to_community,
        )
