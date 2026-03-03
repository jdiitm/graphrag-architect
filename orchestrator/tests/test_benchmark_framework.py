from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
BENCHMARKS_DIR = REPO_ROOT / "tests" / "benchmarks"
GO_BENCH_DIR = BENCHMARKS_DIR / "go"
PYTHON_BENCH_DIR = BENCHMARKS_DIR / "python"
DOCS_DIR = REPO_ROOT / "docs"


class TestBenchmarkDirectoryStructure:

    def test_benchmarks_directory_exists(self) -> None:
        assert BENCHMARKS_DIR.is_dir(), (
            f"Expected tests/benchmarks/ directory at {BENCHMARKS_DIR}"
        )

    def test_go_benchmarks_directory_exists(self) -> None:
        assert GO_BENCH_DIR.is_dir(), (
            "Expected tests/benchmarks/go/ directory"
        )

    def test_python_benchmarks_directory_exists(self) -> None:
        assert PYTHON_BENCH_DIR.is_dir(), (
            "Expected tests/benchmarks/python/ directory"
        )


class TestGoBenchmarkFiles:

    def test_ingestion_bench_file_exists(self) -> None:
        bench_file = GO_BENCH_DIR / "ingestion_bench_test.go"
        assert bench_file.is_file(), (
            "Expected Go benchmark file: ingestion_bench_test.go"
        )

    def test_go_mod_exists(self) -> None:
        go_mod = GO_BENCH_DIR / "go.mod"
        assert go_mod.is_file(), (
            "Expected go.mod in tests/benchmarks/go/"
        )

    def test_bench_file_contains_benchmark_functions(self) -> None:
        bench_file = GO_BENCH_DIR / "ingestion_bench_test.go"
        content = bench_file.read_text(encoding="utf-8")
        assert "func Benchmark" in content, (
            "Go benchmark file must contain Benchmark functions"
        )

    def test_bench_file_has_at_least_three_benchmarks(self) -> None:
        bench_file = GO_BENCH_DIR / "ingestion_bench_test.go"
        content = bench_file.read_text(encoding="utf-8")
        bench_count = content.count("func Benchmark")
        assert bench_count >= 3, (
            f"Expected at least 3 Go benchmarks, found {bench_count}"
        )

    def test_bench_file_uses_testing_b(self) -> None:
        bench_file = GO_BENCH_DIR / "ingestion_bench_test.go"
        content = bench_file.read_text(encoding="utf-8")
        assert "*testing.B" in content, (
            "Go benchmarks must accept *testing.B parameter"
        )

    def test_bench_file_reports_allocs(self) -> None:
        bench_file = GO_BENCH_DIR / "ingestion_bench_test.go"
        content = bench_file.read_text(encoding="utf-8")
        assert "b.ResetTimer()" in content, (
            "Go benchmarks should call b.ResetTimer() to exclude setup"
        )


class TestPythonProfilingScripts:

    def test_profile_query_script_exists(self) -> None:
        script = PYTHON_BENCH_DIR / "profile_query.py"
        assert script.is_file(), (
            "Expected Python profiling script: profile_query.py"
        )

    def test_profile_query_uses_cprofile(self) -> None:
        script = PYTHON_BENCH_DIR / "profile_query.py"
        content = script.read_text(encoding="utf-8")
        assert "cProfile" in content, (
            "Python profiling script must use cProfile"
        )

    def test_profile_query_has_main_entrypoint(self) -> None:
        script = PYTHON_BENCH_DIR / "profile_query.py"
        content = script.read_text(encoding="utf-8")
        assert 'if __name__ == "__main__"' in content, (
            "Python profiling script must have __main__ guard"
        )

    def test_profile_query_accepts_arguments(self) -> None:
        script = PYTHON_BENCH_DIR / "profile_query.py"
        content = script.read_text(encoding="utf-8")
        assert "argparse" in content, (
            "Python profiling script should use argparse for CLI args"
        )


class TestBenchmarkOrchestration:

    def test_run_benchmarks_script_exists(self) -> None:
        script = BENCHMARKS_DIR / "run_benchmarks.sh"
        assert script.is_file(), (
            "Expected orchestration script: run_benchmarks.sh"
        )

    def test_run_benchmarks_script_is_executable(self) -> None:
        script = BENCHMARKS_DIR / "run_benchmarks.sh"
        import os
        assert os.access(script, os.X_OK), (
            "run_benchmarks.sh must be executable"
        )

    def test_run_benchmarks_invokes_go_bench(self) -> None:
        script = BENCHMARKS_DIR / "run_benchmarks.sh"
        content = script.read_text(encoding="utf-8")
        assert "go test" in content, (
            "run_benchmarks.sh must invoke Go benchmarks via 'go test'"
        )

    def test_run_benchmarks_invokes_python_profiling(self) -> None:
        script = BENCHMARKS_DIR / "run_benchmarks.sh"
        content = script.read_text(encoding="utf-8")
        assert "profile_query.py" in content, (
            "run_benchmarks.sh must invoke Python profiling script"
        )


class TestCapacityPlanningDoc:

    @pytest.fixture(name="capacity_doc")
    def _capacity_doc(self) -> str:
        path = DOCS_DIR / "capacity-planning.md"
        assert path.is_file(), (
            "Expected docs/capacity-planning.md"
        )
        return path.read_text(encoding="utf-8")

    def test_document_exists(self) -> None:
        path = DOCS_DIR / "capacity-planning.md"
        assert path.is_file()

    def test_has_1x_scale_section(self, capacity_doc: str) -> None:
        assert "1x" in capacity_doc, (
            "Capacity planning must include 1x baseline scale"
        )

    def test_has_10x_scale_section(self, capacity_doc: str) -> None:
        assert "10x" in capacity_doc, (
            "Capacity planning must include 10x growth scale"
        )

    def test_has_100x_scale_section(self, capacity_doc: str) -> None:
        assert "100x" in capacity_doc, (
            "Capacity planning must include 100x enterprise scale"
        )

    def test_has_resource_calculations(self, capacity_doc: str) -> None:
        resource_terms = ["CPU", "Memory", "Storage"]
        for term in resource_terms:
            assert term in capacity_doc, (
                f"Capacity planning must include {term} calculations"
            )

    def test_covers_all_components(self, capacity_doc: str) -> None:
        components = ["Orchestrator", "Neo4j", "Kafka", "Redis"]
        for component in components:
            assert component in capacity_doc, (
                f"Capacity planning must cover {component}"
            )

    def test_has_bottleneck_analysis(self, capacity_doc: str) -> None:
        assert "Bottleneck" in capacity_doc or "bottleneck" in capacity_doc, (
            "Capacity planning must include bottleneck analysis"
        )
