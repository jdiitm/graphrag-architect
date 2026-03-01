from __future__ import annotations

import subprocess
import sys

import pytest


class TestProcessPoolSafety:

    def test_pool_max_workers_capped(self) -> None:
        from orchestrator.app.graph_builder import _PROCESS_POOL_MAX_WORKERS
        assert _PROCESS_POOL_MAX_WORKERS <= 8, (
            f"Process pool max workers ({_PROCESS_POOL_MAX_WORKERS}) must be "
            f"capped to prevent OOM on large codebases"
        )

    def test_pool_max_workers_from_env(self) -> None:
        code = (
            "import os; os.environ['AST_POOL_WORKERS']='2'; "
            "from orchestrator.app.graph_builder import "
            "_PROCESS_POOL_MAX_WORKERS; "
            "assert _PROCESS_POOL_MAX_WORKERS == 2"
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, result.stderr

    def test_pool_env_value_clamped_to_ceiling(self) -> None:
        code = (
            "import os; os.environ['AST_POOL_WORKERS']='999'; "
            "from orchestrator.app.graph_builder import "
            "_PROCESS_POOL_MAX_WORKERS; "
            "assert _PROCESS_POOL_MAX_WORKERS <= 8, "
            "f'Got {_PROCESS_POOL_MAX_WORKERS}'"
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, result.stderr
