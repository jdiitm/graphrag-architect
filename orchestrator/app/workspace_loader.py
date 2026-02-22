import os
from pathlib import PurePosixPath
from typing import Dict, List


EXCLUDED_DIRS: frozenset = frozenset({
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

INCLUDED_EXTENSIONS: frozenset = frozenset({
    ".go",
    ".py",
    ".yaml",
    ".yml",
})

MAX_FILE_SIZE_BYTES: int = 1_048_576


def load_directory(directory_path: str) -> List[Dict[str, str]]:
    root = os.path.abspath(directory_path)
    if not os.path.isdir(root):
        return []

    results: List[Dict[str, str]] = []

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [
            d for d in dirnames
            if d not in EXCLUDED_DIRS
        ]

        for filename in filenames:
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
            results.append({"path": normalized, "content": content})

    results.sort(key=lambda entry: entry["path"])
    return results
