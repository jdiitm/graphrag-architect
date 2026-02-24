from __future__ import annotations

import pytest

from orchestrator.app.lazy_traversal import (
    LazyTraversalTool,
    TraversalResult,
)


class TestLazyTraversalGrepSearch:

    @pytest.mark.asyncio
    async def test_grep_returns_matching_lines(self, tmp_path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "main.go").write_text("package main\nfunc handleAuth() {}\n")
        (repo / "utils.go").write_text("package utils\nfunc helper() {}\n")

        tool = LazyTraversalTool(str(repo))
        result = await tool.grep("handleAuth")
        assert isinstance(result, TraversalResult)
        assert len(result.matches) >= 1
        assert any("handleAuth" in m.line for m in result.matches)

    @pytest.mark.asyncio
    async def test_grep_no_matches_returns_empty(self, tmp_path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "main.go").write_text("package main\n")

        tool = LazyTraversalTool(str(repo))
        result = await tool.grep("nonexistent_symbol")
        assert result.matches == []

    @pytest.mark.asyncio
    async def test_grep_respects_file_pattern(self, tmp_path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "main.go").write_text("func target() {}\n")
        (repo / "main.py").write_text("def target(): pass\n")

        tool = LazyTraversalTool(str(repo))
        result = await tool.grep("target", file_glob="*.go")
        assert all(m.path.endswith(".go") for m in result.matches)


class TestLazyTraversalParseFile:

    @pytest.mark.asyncio
    async def test_parse_go_file_returns_structure(self, tmp_path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "main.go").write_text(
            "package main\n\n"
            "import \"net/http\"\n\n"
            "func main() {\n"
            "    http.ListenAndServe(\":8080\", nil)\n"
            "}\n"
        )

        tool = LazyTraversalTool(str(repo))
        result = await tool.parse_file("main.go")
        assert result.file_path == "main.go"
        assert result.content is not None
        assert "package main" in result.content

    @pytest.mark.asyncio
    async def test_parse_missing_file_returns_error(self, tmp_path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()

        tool = LazyTraversalTool(str(repo))
        result = await tool.parse_file("nonexistent.go")
        assert result.error is not None


class TestLazyTraversalListFiles:

    @pytest.mark.asyncio
    async def test_list_files_returns_matching_paths(self, tmp_path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "main.go").write_text("package main\n")
        sub = repo / "pkg"
        sub.mkdir()
        (sub / "util.go").write_text("package pkg\n")
        (sub / "readme.md").write_text("# readme\n")

        tool = LazyTraversalTool(str(repo))
        result = await tool.list_files("*.go")
        assert len(result) >= 2
        assert any("main.go" in p for p in result)
        assert any("util.go" in p for p in result)
        assert not any("readme.md" in p for p in result)
