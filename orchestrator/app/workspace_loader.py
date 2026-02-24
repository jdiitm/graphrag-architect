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


def _iter_eligible_paths(root: str) -> Generator[str, None, None]:
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

            relative = os.path.relpath(full_path, root)
            yield str(PurePosixPath(*relative.split(os.sep)))


def _iter_files(root: str) -> Generator[Dict[str, str], None, None]:
    for rel_path in _iter_eligible_paths(root):
        full_path = os.path.join(root, rel_path.replace("/", os.sep))
        try:
            with open(full_path, encoding="utf-8") as fh:
                content = fh.read()
        except (OSError, UnicodeDecodeError):
            continue
        yield {"path": rel_path, "content": content}


def load_directory_chunked(
    directory_path: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_total_bytes: Optional[int] = None,
    skipped: Optional[List[str]] = None,
) -> Generator[List[Dict[str, str]], None, None]:
    if not directory_path or not directory_path.strip():
        return

    root = os.path.abspath(directory_path)
    if not os.path.isdir(root):
        return

    chunk: List[Dict[str, str]] = []
    total_bytes = 0
    loaded_paths: Optional[set] = set() if skipped is not None else None
    limit_hit = False

    for entry in _iter_files(root):
        entry_bytes = len(entry["content"].encode("utf-8"))

        if max_total_bytes is not None and total_bytes + entry_bytes > max_total_bytes:
            limit_hit = True
            break

        chunk.append(entry)
        total_bytes += entry_bytes
        if loaded_paths is not None:
            loaded_paths.add(entry["path"])

        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []

    if chunk:
        yield chunk

    if limit_hit:
        if skipped is not None and loaded_paths is not None:
            for path in _iter_eligible_paths(root):
                if path not in loaded_paths:
                    skipped.append(path)
        logger.warning(
            "Workspace loading stopped at %d bytes (limit: %d). "
            "Remaining files skipped to prevent OOM.",
            total_bytes, max_total_bytes,
        )


def _default_max_bytes() -> int:
    raw = os.environ.get("WORKSPACE_MAX_BYTES", "104857600")
    try:
        return int(raw)
    except ValueError:
        return 104857600


def load_directory(
    directory_path: str,
    max_total_bytes: Optional[int] = None,
    skipped: Optional[List[str]] = None,
) -> List[Dict[str, str]]:
    effective_max = max_total_bytes if max_total_bytes is not None else _default_max_bytes()
    results: List[Dict[str, str]] = []
    for chunk in load_directory_chunked(
        directory_path, chunk_size=1000,
        max_total_bytes=effective_max, skipped=skipped,
    ):
        results.extend(chunk)
    results.sort(key=lambda entry: entry["path"])
    return results
