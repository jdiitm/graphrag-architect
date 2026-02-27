from __future__ import annotations

import asyncio
import fnmatch
import logging
import os
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, Tuple, runtime_checkable

_logger = logging.getLogger(__name__)


def _build_undirected_adjacency(
    edges: List[Dict[str, Any]],
) -> Tuple[Dict[str, List[str]], set]:
    adjacency: Dict[str, List[str]] = defaultdict(list)
    nodes: set[str] = set()
    for edge in edges:
        src, tgt = edge.get("source", ""), edge.get("target", "")
        if src and tgt:
            adjacency[src].append(tgt)
            adjacency[tgt].append(src)
            nodes.add(src)
            nodes.add(tgt)
    return adjacency, nodes


def _ppr_iterate(
    nodes: set,
    adjacency: Dict[str, List[str]],
    personalization: Dict[str, float],
    iterations: int,
    damping: float,
) -> Dict[str, float]:
    scores: Dict[str, float] = {n: personalization.get(n, 0.0) for n in nodes}
    out_degree = {n: len(adjacency[n]) for n in nodes}
    for _ in range(iterations):
        new_scores: Dict[str, float] = {}
        for node in nodes:
            rank_sum = sum(
                scores[nb] / out_degree[nb]
                for nb in adjacency[node]
                if out_degree[nb] > 0
            )
            new_scores[node] = (
                (1.0 - damping) * personalization.get(node, 0.0)
                + damping * rank_sum
            )
        total = sum(new_scores.values())
        if total > 0:
            for node in nodes:
                new_scores[node] /= total
        scores = new_scores
    return scores


_DEFAULT_MAX_EDGES = 2000


@runtime_checkable
class PageRankStrategy(Protocol):
    def rank(
        self,
        edges: List[Dict[str, Any]],
        seed_nodes: List[str],
        top_n: int = 50,
    ) -> List[Tuple[str, float]]: ...


class LocalPageRankStrategy:
    def __init__(
        self,
        iterations: int = 20,
        damping: float = 0.85,
        max_edges: int = _DEFAULT_MAX_EDGES,
    ) -> None:
        self._iterations = iterations
        self._damping = damping
        self._max_edges = max_edges

    def rank(
        self,
        edges: List[Dict[str, Any]],
        seed_nodes: List[str],
        top_n: int = 50,
    ) -> List[Tuple[str, float]]:
        if not edges:
            return []

        bounded = edges[:self._max_edges] if len(edges) > self._max_edges else edges
        adjacency, nodes = _build_undirected_adjacency(bounded)
        if not nodes:
            return []

        valid_seeds = [s for s in seed_nodes if s in nodes] or list(nodes)
        seed_weight = 1.0 / len(valid_seeds)
        personalization = {s: seed_weight for s in valid_seeds}

        scores = _ppr_iterate(
            nodes, adjacency, personalization,
            self._iterations, self._damping,
        )
        ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        return ranked[:top_n]


_GDS_EDGE_THRESHOLD = 500

_GDS_PROJECT_QUERY = (
    "CALL gds.graph.project.cypher("
    "$graph_name, $node_query, $rel_query"
    ") YIELD graphName, nodeCount, relationshipCount"
)

_GDS_PAGERANK_QUERY = (
    "CALL gds.pageRank.stream($graph_name, {"
    "  maxIterations: $max_iterations,"
    "  dampingFactor: $damping_factor"
    "}) YIELD nodeId, score "
    "WITH gds.util.asNode(nodeId) AS node, score "
    "RETURN node.name AS name, score "
    "ORDER BY score DESC "
    "LIMIT $top_n"
)

_GDS_GRAPH_DROP = "CALL gds.graph.drop($graph_name) YIELD graphName"


def _sanitize_cypher_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def build_gds_node_query(
    seed_names: List[str], tenant_id: str = "",
) -> str:
    escaped = ", ".join(
        f"'{_sanitize_cypher_string(n)}'" for n in seed_names
    )
    tenant_clause = ""
    if tenant_id:
        safe_tid = _sanitize_cypher_string(tenant_id)
        tenant_clause = f" AND n.tenant_id = '{safe_tid}'"
    return (
        f"MATCH (n)-[r]-(m) "
        f"WHERE n.name IN [{escaped}]{tenant_clause} "
        f"WITH collect(DISTINCT n) + collect(DISTINCT m) AS all_nodes "
        f"UNWIND all_nodes AS node "
        f"RETURN DISTINCT id(node) AS id"
    )


def build_gds_rel_query(
    seed_names: List[str], tenant_id: str = "",
) -> str:
    escaped = ", ".join(
        f"'{_sanitize_cypher_string(n)}'" for n in seed_names
    )
    tenant_clause = ""
    if tenant_id:
        safe_tid = _sanitize_cypher_string(tenant_id)
        tenant_clause = (
            f" AND n.tenant_id = '{safe_tid}'"
            f" AND m.tenant_id = '{safe_tid}'"
        )
    return (
        f"MATCH (n)-[r]->(m) "
        f"WHERE (n.name IN [{escaped}] OR m.name IN [{escaped}])"
        f"{tenant_clause} "
        f"AND r.tombstoned_at IS NULL "
        f"RETURN DISTINCT id(n) AS source, id(m) AS target"
    )


class GDSPageRankStrategy:
    def __init__(
        self,
        driver: Any = None,
        iterations: int = 20,
        damping: float = 0.85,
    ) -> None:
        self._driver = driver
        self._iterations = iterations
        self._damping = damping
        self._fallback = LocalPageRankStrategy(
            iterations=iterations, damping=damping,
        )

    def rank(
        self,
        edges: List[Dict[str, Any]],
        seed_nodes: List[str],
        top_n: int = 50,
    ) -> List[Tuple[str, float]]:
        return self._fallback.rank(edges, seed_nodes, top_n)

    async def rank_async(
        self,
        seed_names: List[str],
        tenant_id: str = "",
        top_n: int = 50,
    ) -> List[Tuple[str, float]]:
        if self._driver is None:
            return []

        graph_name = f"ppr_{uuid.uuid4().hex[:12]}"
        try:
            node_query = build_gds_node_query(seed_names, tenant_id)
            rel_query = build_gds_rel_query(seed_names, tenant_id)
            await self._project_subgraph(graph_name, node_query, rel_query)
            return await self._stream_pagerank(graph_name, top_n)
        except Exception:
            _logger.debug("GDS PageRank unavailable, returning empty", exc_info=True)
            return []
        finally:
            await self._safe_drop_graph(graph_name)

    async def _project_subgraph(
        self, graph_name: str, node_query: str, rel_query: str,
    ) -> None:
        async def _tx(tx: Any) -> None:
            await tx.run(
                _GDS_PROJECT_QUERY,
                graph_name=graph_name,
                node_query=node_query,
                rel_query=rel_query,
            )

        async with self._driver.session() as session:
            await session.execute_write(_tx)

    async def _stream_pagerank(
        self, graph_name: str, top_n: int,
    ) -> List[Tuple[str, float]]:
        async def _tx(tx: Any) -> list:
            result = await tx.run(
                _GDS_PAGERANK_QUERY,
                graph_name=graph_name,
                max_iterations=self._iterations,
                damping_factor=self._damping,
                top_n=top_n,
            )
            return await result.data()

        async with self._driver.session() as session:
            records = await session.execute_read(_tx)
        return [(r["name"], r["score"]) for r in records]

    async def _safe_drop_graph(self, graph_name: str) -> None:
        try:
            async def _tx(tx: Any) -> None:
                await tx.run(_GDS_GRAPH_DROP, graph_name=graph_name)

            async with self._driver.session() as session:
                await session.execute_write(_tx)
        except Exception:
            _logger.debug("Failed to drop GDS graph %s", graph_name, exc_info=True)


async def gds_pagerank_filter(
    driver: Any,
    hop_records: List[Dict[str, Any]],
    seed_names: List[str],
    tenant_id: str = "",
    top_n: int = 50,
) -> List[Dict[str, Any]]:
    if len(hop_records) <= _GDS_EDGE_THRESHOLD:
        ranked = LocalPageRankStrategy().rank(hop_records, seed_names, top_n)
        top_nodes = {node for node, _score in ranked}
        return [
            rec for rec in hop_records
            if rec.get("source") in top_nodes or rec.get("target") in top_nodes
        ]

    strategy = GDSPageRankStrategy(driver=driver)
    ranked = await strategy.rank_async(seed_names, tenant_id, top_n)

    if ranked:
        top_nodes = {name for name, _score in ranked}
        return [
            rec for rec in hop_records
            if rec.get("source") in top_nodes or rec.get("target") in top_nodes
        ]

    ranked = LocalPageRankStrategy().rank(hop_records, seed_names, top_n)
    top_nodes = {node for node, _score in ranked}
    return [
        rec for rec in hop_records
        if rec.get("source") in top_nodes or rec.get("target") in top_nodes
    ]


_DEFAULT_STRATEGY = LocalPageRankStrategy()


def personalized_pagerank(
    edges: List[Dict[str, Any]],
    seed_nodes: List[str],
    iterations: int = 20,
    damping: float = 0.85,
    top_n: int = 50,
    max_edges: int = _DEFAULT_MAX_EDGES,
    strategy: Optional[PageRankStrategy] = None,
) -> List[Tuple[str, float]]:
    if strategy is not None:
        return strategy.rank(edges, seed_nodes, top_n)

    if not edges:
        return []

    bounded_edges = edges[:max_edges] if len(edges) > max_edges else edges

    adjacency, nodes = _build_undirected_adjacency(bounded_edges)
    if not nodes:
        return []

    valid_seeds = [s for s in seed_nodes if s in nodes] or list(nodes)
    seed_weight = 1.0 / len(valid_seeds)
    personalization = {s: seed_weight for s in valid_seeds}

    scores = _ppr_iterate(nodes, adjacency, personalization, iterations, damping)
    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    return ranked[:top_n]


@dataclass(frozen=True)
class GrepMatch:
    path: str
    line_number: int
    line: str


@dataclass
class TraversalResult:
    matches: List[GrepMatch] = field(default_factory=list)


@dataclass
class FileParseResult:
    file_path: str
    content: Optional[str] = None
    error: Optional[str] = None


class LazyTraversalTool:
    def __init__(self, repo_root: str) -> None:
        self._root = repo_root

    async def grep(
        self,
        pattern: str,
        file_glob: str = "",
    ) -> TraversalResult:
        matches = await asyncio.to_thread(
            self._grep_sync, pattern, file_glob,
        )
        return TraversalResult(matches=matches)

    def _grep_sync(
        self, pattern: str, file_glob: str,
    ) -> List[GrepMatch]:
        results: List[GrepMatch] = []
        for dirpath, _, filenames in os.walk(self._root):
            for fname in filenames:
                if file_glob and not fnmatch.fnmatch(fname, file_glob):
                    continue
                fpath = os.path.join(dirpath, fname)
                rel = os.path.relpath(fpath, self._root)
                try:
                    with open(fpath, encoding="utf-8", errors="replace") as fh:
                        for lineno, line in enumerate(fh, start=1):
                            if pattern in line:
                                results.append(
                                    GrepMatch(
                                        path=rel,
                                        line_number=lineno,
                                        line=line.rstrip("\n"),
                                    )
                                )
                except (OSError, UnicodeDecodeError):
                    continue
        return results

    async def parse_file(self, relative_path: str) -> FileParseResult:
        full = os.path.join(self._root, relative_path)
        if not os.path.isfile(full):
            return FileParseResult(
                file_path=relative_path,
                error=f"File not found: {relative_path}",
            )
        try:
            content = await asyncio.to_thread(self._read_file, full)
            return FileParseResult(file_path=relative_path, content=content)
        except Exception as exc:
            return FileParseResult(
                file_path=relative_path, error=str(exc),
            )

    @staticmethod
    def _read_file(path: str) -> str:
        with open(path, encoding="utf-8") as fh:
            return fh.read()

    async def list_files(self, glob_pattern: str = "*") -> List[str]:
        return await asyncio.to_thread(
            self._list_files_sync, glob_pattern,
        )

    def _list_files_sync(self, glob_pattern: str) -> List[str]:
        matched: List[str] = []
        for dirpath, _, filenames in os.walk(self._root):
            for fname in filenames:
                if fnmatch.fnmatch(fname, glob_pattern):
                    fpath = os.path.join(dirpath, fname)
                    matched.append(os.path.relpath(fpath, self._root))
        return sorted(matched)
