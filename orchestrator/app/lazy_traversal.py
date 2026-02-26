from __future__ import annotations

import asyncio
import fnmatch
import os
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


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


def personalized_pagerank(
    edges: List[Dict[str, Any]],
    seed_nodes: List[str],
    iterations: int = 20,
    damping: float = 0.85,
    top_n: int = 50,
) -> List[Tuple[str, float]]:
    if not edges:
        return []

    adjacency, nodes = _build_undirected_adjacency(edges)
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
