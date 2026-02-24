from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Set

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
