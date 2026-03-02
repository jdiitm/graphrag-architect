from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple

from orchestrator.app.community_summary import generate_summary
from orchestrator.app.graph_embeddings import GraphTopology
from orchestrator.app.macro_node import MacroNode
from orchestrator.app.semantic_partitioner import (
    PartitionResult,
    SemanticPartitioner,
)

logger = logging.getLogger(__name__)

DEFAULT_MIN_COMMUNITY_SIZE = 3


@dataclass
class CondensationResult:
    macro_nodes: List[MacroNode] = field(default_factory=list)
    inter_community_edges: List[Tuple[str, str]] = field(default_factory=list)
    node_to_macro: Dict[str, str] = field(default_factory=dict)


def _detect_communities(
    topology: GraphTopology,
) -> PartitionResult:
    partitioner = SemanticPartitioner()
    return partitioner.partition(topology)


def _build_macro_nodes(
    partition: PartitionResult,
    topology: GraphTopology,
    tenant_id: str,
    min_community_size: int,
) -> List[MacroNode]:
    macro_nodes: List[MacroNode] = []
    for community in partition.communities:
        if len(community.members) < min_community_size:
            continue
        member_names = sorted(community.members)
        member_types = ["Node"] * len(member_names)
        summary = generate_summary(
            member_names=member_names,
            member_types=member_types,
        )
        macro = MacroNode(
            node_id=f"macro-{community.community_id}",
            community_id=community.community_id,
            tenant_id=tenant_id,
            member_ids=community.members,
            summary_text=summary,
            member_count=len(community.members),
        )
        macro_nodes.append(macro)
    return macro_nodes


def _build_member_to_macro_map(
    macro_nodes: List[MacroNode],
) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for mn in macro_nodes:
        for member in mn.member_ids:
            mapping[member] = mn.node_id
    return mapping


def _find_inter_community_edges(
    topology: GraphTopology,
    member_to_macro: Dict[str, str],
) -> List[Tuple[str, str]]:
    edge_pairs: Set[Tuple[str, str]] = set()
    for node in topology.nodes:
        src_macro = member_to_macro.get(node)
        if src_macro is None:
            continue
        for neighbor in topology.neighbors(node):
            tgt_macro = member_to_macro.get(neighbor)
            if tgt_macro is None or tgt_macro == src_macro:
                continue
            pair = (min(src_macro, tgt_macro), max(src_macro, tgt_macro))
            edge_pairs.add(pair)

    return sorted(edge_pairs)


def condense_graph(
    topology: GraphTopology,
    tenant_id: str,
    min_community_size: int = DEFAULT_MIN_COMMUNITY_SIZE,
) -> CondensationResult:
    partition = _detect_communities(topology)
    macro_nodes = _build_macro_nodes(
        partition, topology, tenant_id, min_community_size,
    )
    if not macro_nodes:
        return CondensationResult()

    node_to_macro = _build_member_to_macro_map(macro_nodes)
    inter_edges = _find_inter_community_edges(topology, node_to_macro)

    logger.info(
        "Condensed %d nodes into %d macro-nodes with %d inter-community edges "
        "(tenant=%s)",
        len(topology.nodes),
        len(macro_nodes),
        len(inter_edges),
        tenant_id,
    )

    return CondensationResult(
        macro_nodes=macro_nodes,
        inter_community_edges=inter_edges,
        node_to_macro=node_to_macro,
    )
