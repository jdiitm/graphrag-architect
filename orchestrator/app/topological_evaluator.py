from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Coroutine, List, Optional


EdgeVerifier = Callable[[List[str]], Coroutine[Any, Any, int]]
PathChecker = Callable[[str, str, int], Coroutine[Any, Any, bool]]


@dataclass(frozen=True)
class TopologicalScore:
    edge_existence_ratio: float
    path_reachable: Optional[bool]
    composite_score: float
    is_hallucinated: bool
    claimed_edges: int
    verified_edges: int


class TopologicalEvaluator:
    def __init__(
        self,
        verify_edges: EdgeVerifier,
        check_path_reachability: Optional[PathChecker] = None,
        alpha: float = 0.6,
        topo_threshold: float = 0.3,
        max_hops: int = 5,
    ) -> None:
        self._verify_edges = verify_edges
        self._check_path = check_path_reachability
        self._alpha = alpha
        self._topo_threshold = topo_threshold
        self._max_hops = max_hops

    async def evaluate_topology(
        self,
        claimed_edge_ids: List[str],
        start_node: Optional[str] = None,
        end_node: Optional[str] = None,
        vector_score: float = 0.0,
    ) -> TopologicalScore:
        if not claimed_edge_ids:
            edge_ratio = 0.0
            verified = 0
        else:
            verified = await self._verify_edges(claimed_edge_ids)
            edge_ratio = verified / len(claimed_edge_ids)

        path_reachable: Optional[bool] = None
        if (
            start_node is not None
            and end_node is not None
            and self._check_path is not None
        ):
            path_reachable = await self._check_path(
                start_node, end_node, self._max_hops,
            )

        path_score = 1.0 if path_reachable is True else 0.0
        topo_score = edge_ratio
        if path_reachable is not None:
            topo_score = (edge_ratio + path_score) / 2.0

        composite = self._alpha * vector_score + (1 - self._alpha) * topo_score
        is_hallucinated = topo_score < self._topo_threshold

        return TopologicalScore(
            edge_existence_ratio=edge_ratio,
            path_reachable=path_reachable,
            composite_score=composite,
            is_hallucinated=is_hallucinated,
            claimed_edges=len(claimed_edge_ids),
            verified_edges=verified if claimed_edge_ids else 0,
        )
