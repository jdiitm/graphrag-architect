from __future__ import annotations

import asyncio
import fnmatch
import os
from dataclasses import dataclass, field
from typing import List, Optional


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
