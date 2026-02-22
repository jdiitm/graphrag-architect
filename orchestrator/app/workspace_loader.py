import logging
import os
from pathlib import PurePosixPath
from typing import Dict, Generator, List, Optional


logger = logging.getLogger(__name__)


EXCLUDED_DIRS: frozenset[str] = frozenset({
    ".git",
    ".venv",
    "__pycache__",
    "node_modules",
    ".mypy_cache",
    ".pytest_cache",
    ".tox",
    ".eggs",
    "venv",
})

INCLUDED_EXTENSIONS: frozenset[str] = frozenset({
    ".go",
    ".py",
    ".yaml",
    ".yml",
})

MAX_FILE_SIZE_BYTES: int = 1_048_576

DEFAULT_CHUNK_SIZE: int = 50


def _iter_files(root: str) -> Generator[Dict[str, str], None, None]:
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [
            d for d in dirnames
            if d not in EXCLUDED_DIRS
        ]

        for filename in sorted(filenames):
            _, ext = os.path.splitext(filename)
            if ext not in INCLUDED_EXTENSIONS:
                continue

            full_path = os.path.join(dirpath, filename)

            try:
                file_size = os.path.getsize(full_path)
            except OSError:
                continue

            if file_size > MAX_FILE_SIZE_BYTES:
                continue

            try:
                with open(full_path, encoding="utf-8") as fh:
                    content = fh.read()
            except (OSError, UnicodeDecodeError):
                continue

            relative = os.path.relpath(full_path, root)
            normalized = str(PurePosixPath(*relative.split(os.sep)))
            yield {"path": normalized, "content": content}


def load_directory_chunked(
    directory_path: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_total_bytes: Optional[int] = None,
) -> Generator[List[Dict[str, str]], None, None]:
    if not directory_path or not directory_path.strip():
        return

    root = os.path.abspath(directory_path)
    if not os.path.isdir(root):
        return

    chunk: List[Dict[str, str]] = []
    total_bytes = 0

    for entry in _iter_files(root):
        entry_bytes = len(entry["content"].encode("utf-8"))

        if max_total_bytes is not None and total_bytes + entry_bytes > max_total_bytes:
            logger.warning(
                "Workspace loading stopped at %d bytes (limit: %d). "
                "Remaining files skipped to prevent OOM.",
                total_bytes, max_total_bytes,
            )
            break

        chunk.append(entry)
        total_bytes += entry_bytes

        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []

    if chunk:
        yield chunk


def load_directory(directory_path: str) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    for chunk in load_directory_chunked(directory_path, chunk_size=1000):
        results.extend(chunk)
    results.sort(key=lambda entry: entry["path"])
    return results
