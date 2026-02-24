import os
from pathlib import Path

import pytest

from orchestrator.app.workspace_loader import (
    EXCLUDED_DIRS,
    INCLUDED_EXTENSIONS,
    MAX_FILE_SIZE_BYTES,
    load_directory,
    load_directory_chunked,
)


@pytest.fixture()
def workspace(tmp_path: Path):
    (tmp_path / "main.go").write_text("package main")
    (tmp_path / "app.py").write_text("print('hello')")
    (tmp_path / "deploy.yaml").write_text("apiVersion: apps/v1")
    (tmp_path / "config.yml").write_text("topic: raw-documents")
    return tmp_path


class TestLoadDirectoryExtensions:

    def test_loads_go_files(self, workspace: Path):
        result = load_directory(str(workspace))
        paths = [f["path"] for f in result]
        assert "main.go" in paths

    def test_loads_python_files(self, workspace: Path):
        result = load_directory(str(workspace))
        paths = [f["path"] for f in result]
        assert "app.py" in paths

    def test_loads_yaml_files(self, workspace: Path):
        result = load_directory(str(workspace))
        paths = [f["path"] for f in result]
        assert "deploy.yaml" in paths

    def test_loads_yml_files(self, workspace: Path):
        result = load_directory(str(workspace))
        paths = [f["path"] for f in result]
        assert "config.yml" in paths

    def test_skips_unsupported_extensions(self, workspace: Path):
        (workspace / "readme.md").write_text("# Hello")
        (workspace / "data.json").write_text("{}")
        (workspace / "image.png").write_bytes(b"\x89PNG")
        result = load_directory(str(workspace))
        paths = [f["path"] for f in result]
        assert "readme.md" not in paths
        assert "data.json" not in paths
        assert "image.png" not in paths

    def test_returns_file_content(self, workspace: Path):
        result = load_directory(str(workspace))
        go_file = next(f for f in result if f["path"] == "main.go")
        assert go_file["content"] == "package main"


class TestLoadDirectoryExcludedDirs:

    def test_skips_git_directory(self, workspace: Path):
        git_dir = workspace / ".git"
        git_dir.mkdir()
        (git_dir / "config.py").write_text("git internal")
        result = load_directory(str(workspace))
        paths = [f["path"] for f in result]
        assert not any(".git" in p for p in paths)

    def test_skips_venv_directory(self, workspace: Path):
        venv_dir = workspace / ".venv"
        venv_dir.mkdir()
        (venv_dir / "activate.py").write_text("venv stuff")
        result = load_directory(str(workspace))
        paths = [f["path"] for f in result]
        assert not any(".venv" in p for p in paths)

    def test_skips_pycache_directory(self, workspace: Path):
        cache_dir = workspace / "__pycache__"
        cache_dir.mkdir()
        (cache_dir / "module.py").write_text("cached bytecode")
        result = load_directory(str(workspace))
        paths = [f["path"] for f in result]
        assert not any("__pycache__" in p for p in paths)

    def test_skips_node_modules_directory(self, workspace: Path):
        nm_dir = workspace / "node_modules"
        nm_dir.mkdir()
        (nm_dir / "index.py").write_text("node stuff")
        result = load_directory(str(workspace))
        paths = [f["path"] for f in result]
        assert not any("node_modules" in p for p in paths)


class TestLoadDirectoryStructure:

    def test_nested_directories(self, tmp_path: Path):
        services = tmp_path / "services" / "auth"
        services.mkdir(parents=True)
        (services / "handler.go").write_text("package auth")
        infra = tmp_path / "infra" / "k8s"
        infra.mkdir(parents=True)
        (infra / "deployment.yaml").write_text("kind: Deployment")
        result = load_directory(str(tmp_path))
        paths = [f["path"] for f in result]
        assert "infra/k8s/deployment.yaml" in paths
        assert "services/auth/handler.go" in paths

    def test_empty_directory(self, tmp_path: Path):
        empty = tmp_path / "empty_project"
        empty.mkdir()
        result = load_directory(str(empty))
        assert result == []

    def test_returns_relative_paths(self, workspace: Path):
        result = load_directory(str(workspace))
        for entry in result:
            assert not os.path.isabs(entry["path"])

    def test_deterministic_sort_order(self, workspace: Path):
        result = load_directory(str(workspace))
        paths = [f["path"] for f in result]
        assert paths == sorted(paths)

    def test_uses_forward_slash_separators(self, tmp_path: Path):
        nested = tmp_path / "a" / "b"
        nested.mkdir(parents=True)
        (nested / "c.go").write_text("package c")
        result = load_directory(str(tmp_path))
        assert result[0]["path"] == "a/b/c.go"


class TestLoadDirectorySizeLimits:

    def test_skips_files_exceeding_max_size(self, tmp_path: Path):
        large_file = tmp_path / "huge.go"
        large_file.write_text("x" * (MAX_FILE_SIZE_BYTES + 1))
        (tmp_path / "small.go").write_text("package small")
        result = load_directory(str(tmp_path))
        paths = [f["path"] for f in result]
        assert "huge.go" not in paths
        assert "small.go" in paths


class TestLoadDirectoryErrorHandling:

    def test_unreadable_file_skipped(self, tmp_path: Path):
        bad_file = tmp_path / "secret.go"
        bad_file.write_text("package secret")
        bad_file.chmod(0o000)
        (tmp_path / "good.go").write_text("package good")
        result = load_directory(str(tmp_path))
        paths = [f["path"] for f in result]
        assert "good.go" in paths
        assert "secret.go" not in paths
        bad_file.chmod(0o644)

    def test_nonexistent_directory_returns_empty(self):
        result = load_directory("/nonexistent/path/that/does/not/exist")
        assert result == []

    def test_empty_string_path_returns_empty(self):
        result = load_directory("")
        assert result == []

    def test_whitespace_only_path_returns_empty(self):
        result = load_directory("   ")
        assert result == []


class TestLoadDirectoryConstants:

    def test_excluded_dirs_contains_expected(self):
        for dirname in (".git", ".venv", "__pycache__", "node_modules"):
            assert dirname in EXCLUDED_DIRS

    def test_included_extensions_contains_expected(self):
        for ext in (".go", ".py", ".yaml", ".yml"):
            assert ext in INCLUDED_EXTENSIONS

    def test_max_file_size_is_one_megabyte(self):
        assert MAX_FILE_SIZE_BYTES == 1_048_576


class TestLoadDirectoryChunked:

    def test_yields_batches_of_specified_size(self, tmp_path: Path):
        for i in range(7):
            (tmp_path / f"file_{i}.go").write_text(f"package f{i}")
        chunks = list(load_directory_chunked(str(tmp_path), chunk_size=3))
        assert len(chunks) == 3
        assert len(chunks[0]) == 3
        assert len(chunks[1]) == 3
        assert len(chunks[2]) == 1

    def test_single_chunk_when_fewer_files_than_chunk_size(self, tmp_path: Path):
        (tmp_path / "a.go").write_text("package a")
        (tmp_path / "b.go").write_text("package b")
        chunks = list(load_directory_chunked(str(tmp_path), chunk_size=10))
        assert len(chunks) == 1
        assert len(chunks[0]) == 2

    def test_empty_directory_yields_nothing(self, tmp_path: Path):
        empty = tmp_path / "empty"
        empty.mkdir()
        chunks = list(load_directory_chunked(str(empty), chunk_size=5))
        assert chunks == []

    def test_chunk_contents_match_full_load(self, workspace: Path):
        full = load_directory(str(workspace))
        chunked_all = []
        for chunk in load_directory_chunked(str(workspace), chunk_size=2):
            chunked_all.extend(chunk)
        assert sorted(chunked_all, key=lambda e: e["path"]) == full

    def test_respects_max_total_bytes(self, tmp_path: Path):
        for i in range(10):
            (tmp_path / f"big_{i}.go").write_text("x" * 1000)
        chunks = list(load_directory_chunked(
            str(tmp_path), chunk_size=5, max_total_bytes=3000
        ))
        total_files = sum(len(c) for c in chunks)
        assert total_files == 3

    def test_invalid_path_yields_nothing(self):
        chunks = list(load_directory_chunked("/nonexistent", chunk_size=5))
        assert chunks == []


class TestLoadWorkspaceFilesDAGNode:

    @pytest.mark.asyncio
    async def test_dag_node_delegates_to_load_directory(self, workspace: Path):
        from orchestrator.app.graph_builder import load_workspace_files

        state = {"directory_path": str(workspace)}
        result = await load_workspace_files(state)
        assert "raw_files" in result
        assert len(result["raw_files"]) == 4
        paths = [f["path"] for f in result["raw_files"]]
        assert "main.go" in paths
        assert "app.py" in paths

    @pytest.mark.asyncio
    async def test_dag_node_returns_empty_for_missing_dir(self):
        from orchestrator.app.graph_builder import load_workspace_files

        state = {"directory_path": "/nonexistent/path"}
        result = await load_workspace_files(state)
        assert result["raw_files"] == []
