from __future__ import annotations

import logging
import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

import numpy as np

logger = logging.getLogger(__name__)

DEFAULT_WALK_LENGTH = 80
DEFAULT_NUM_WALKS = 10
DEFAULT_EMBEDDING_DIM = 128
DEFAULT_P = 1.0
DEFAULT_Q = 0.5


@dataclass(frozen=True)
class Node2VecConfig:
    walk_length: int = DEFAULT_WALK_LENGTH
    num_walks: int = DEFAULT_NUM_WALKS
    embedding_dim: int = DEFAULT_EMBEDDING_DIM
    p: float = DEFAULT_P
    q: float = DEFAULT_Q
    seed: Optional[int] = None


@dataclass
class GraphTopology:
    nodes: List[str] = field(default_factory=list)
    adjacency: Dict[str, List[str]] = field(default_factory=dict)

    def neighbors(self, node: str) -> List[str]:
        return self.adjacency.get(node, [])


def _compute_transition_probs(
    prev: Optional[str],
    current: str,
    neighbors: List[str],
    topology: GraphTopology,
    p: float,
    q: float,
) -> List[float]:
    if not neighbors:
        return []
    weights: List[float] = []
    prev_neighbors: Set[str] = set()
    if prev is not None:
        prev_neighbors = set(topology.neighbors(prev))
    for nbr in neighbors:
        if nbr == prev:
            weights.append(1.0 / p)
        elif nbr in prev_neighbors:
            weights.append(1.0)
        else:
            weights.append(1.0 / q)
    total = sum(weights)
    if total == 0:
        return [1.0 / len(neighbors)] * len(neighbors)
    return [w / total for w in weights]


def _biased_random_walk(
    start: str,
    topology: GraphTopology,
    walk_length: int,
    p: float,
    q: float,
    rng: random.Random,
) -> List[str]:
    walk = [start]
    for _ in range(walk_length - 1):
        current = walk[-1]
        neighbors = topology.neighbors(current)
        if not neighbors:
            break
        prev = walk[-2] if len(walk) >= 2 else None
        probs = _compute_transition_probs(
            prev, current, neighbors, topology, p, q,
        )
        chosen = rng.choices(neighbors, weights=probs, k=1)[0]
        walk.append(chosen)
    return walk


def generate_walks(
    topology: GraphTopology,
    config: Node2VecConfig,
) -> List[List[str]]:
    rng = random.Random(config.seed)
    walks: List[List[str]] = []
    for _ in range(config.num_walks):
        nodes = list(topology.nodes)
        rng.shuffle(nodes)
        for node in nodes:
            walk = _biased_random_walk(
                node, topology,
                walk_length=config.walk_length,
                p=config.p, q=config.q,
                rng=rng,
            )
            walks.append(walk)
    return walks


def _hash_embedding(
    node: str,
    walks: List[List[str]],
    dim: int,
) -> List[float]:
    embedding = [0.0] * dim
    count = 0
    for walk in walks:
        for i, w_node in enumerate(walk):
            if w_node == node:
                bucket = hash(f"{i}:{count}") % dim
                embedding[bucket] += 1.0
                count += 1
    norm = sum(v * v for v in embedding) ** 0.5
    if norm > 0:
        embedding = [v / norm for v in embedding]
    return embedding


class Node2VecEmbedder:
    def __init__(self, config: Optional[Node2VecConfig] = None) -> None:
        self._config = config or Node2VecConfig()

    def embed(
        self,
        topology: GraphTopology,
    ) -> Dict[str, List[float]]:
        walks = generate_walks(topology, self._config)
        embeddings: Dict[str, List[float]] = {}
        for node in topology.nodes:
            embeddings[node] = _hash_embedding(
                node, walks, self._config.embedding_dim,
            )
        return embeddings

    def embed_incremental(
        self,
        topology: GraphTopology,
        changed_nodes: List[str],
        existing_embeddings: Dict[str, List[float]],
    ) -> Dict[str, List[float]]:
        affected: Set[str] = set(changed_nodes)
        for node in changed_nodes:
            affected.update(topology.neighbors(node))
        walks = generate_walks(topology, self._config)
        result = dict(existing_embeddings)
        for node in affected:
            if node in topology.nodes:
                result[node] = _hash_embedding(
                    node, walks, self._config.embedding_dim,
                )
        return result


def compute_centroid(vectors: List[List[float]]) -> List[float]:
    if not vectors:
        return []
    arr = np.array(vectors, dtype=np.float64)
    return np.mean(arr, axis=0).tolist()


def _cosine_sim(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    va = np.asarray(a, dtype=np.float64)
    vb = np.asarray(b, dtype=np.float64)
    norm_a = np.linalg.norm(va)
    norm_b = np.linalg.norm(vb)
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return float(np.dot(va, vb) / (norm_a * norm_b))


def hybrid_score(
    text_score: float,
    structural_score: float,
    text_weight: float = 0.7,
    structural_weight: float = 0.3,
) -> float:
    return text_weight * text_score + structural_weight * structural_score


@dataclass(frozen=True)
class _ScoredResult:
    id: str
    score: float
    metadata: Dict[str, Any]


def _batch_cosine_similarities(
    ids: List[str],
    structural_embeddings: Dict[str, List[float]],
    query_vec: "np.ndarray",
    dim: int,
) -> "np.ndarray":
    emb_matrix = []
    has_emb = []
    for rid in ids:
        emb = structural_embeddings.get(rid)
        if emb is not None:
            emb_matrix.append(emb)
            has_emb.append(True)
        else:
            emb_matrix.append([0.0] * dim)
            has_emb.append(False)

    emb_arr = np.array(emb_matrix, dtype=np.float64)
    norms = np.linalg.norm(emb_arr, axis=1)
    sims = np.zeros(len(ids), dtype=np.float64)
    query_norm = np.linalg.norm(query_vec)
    valid = np.array(has_emb) & (norms > 0.0) & (query_norm > 0.0)
    if valid.any():
        dots = emb_arr[valid] @ query_vec
        sims[valid] = np.maximum(0.0, dots / (norms[valid] * query_norm))
    return sims


def rerank_with_structural(
    text_results: List[Any],
    structural_embeddings: Dict[str, List[float]],
    query_structural: List[float],
    text_weight: float = 0.7,
    structural_weight: float = 0.3,
) -> List[Any]:
    if not text_results:
        return []

    from orchestrator.app.vector_store import SearchResult

    ids = [r.id for r in text_results]
    text_scores = np.array([r.score for r in text_results], dtype=np.float64)
    metadatas = [r.metadata for r in text_results]

    if not query_structural:
        order = np.argsort(-text_scores)
        return [
            SearchResult(id=ids[i], score=float(text_scores[i]), metadata=metadatas[i])
            for i in order
        ]

    query_vec = np.asarray(query_structural, dtype=np.float64)
    struct_sims = _batch_cosine_similarities(
        ids, structural_embeddings, query_vec, len(query_structural),
    )
    combined = text_weight * text_scores + structural_weight * struct_sims
    order = np.argsort(-combined)

    return [
        SearchResult(id=ids[i], score=float(combined[i]), metadata=metadatas[i])
        for i in order
    ]
