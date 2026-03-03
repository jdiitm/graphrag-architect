import os
import stat

import pytest


_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
_BENCH_DIR = os.path.join(_REPO_ROOT, "tests", "benchmarks")
_GO_BENCH_DIR = os.path.join(_BENCH_DIR, "go")
_PY_BENCH_DIR = os.path.join(_BENCH_DIR, "python")
_DOCS_DIR = os.path.join(_REPO_ROOT, "docs")


class TestBenchmarkDirectoryStructure:
    def test_benchmarks_directory_exists(self):
        assert os.path.isdir(_BENCH_DIR), (
            f"tests/benchmarks/ directory missing: {_BENCH_DIR}"
        )

    def test_go_benchmarks_directory_exists(self):
        assert os.path.isdir(_GO_BENCH_DIR), (
            "tests/benchmarks/go/ directory missing"
        )

    def test_python_benchmarks_directory_exists(self):
        assert os.path.isdir(_PY_BENCH_DIR), (
            "tests/benchmarks/python/ directory missing"
        )


class TestGoBenchmarkFiles:
    def test_go_benchmark_file_exists(self):
        bench_files = [
            f for f in os.listdir(_GO_BENCH_DIR)
            if f.endswith("_test.go")
        ]
        assert bench_files, "No Go benchmark files (*_test.go) found"

    def test_ingestion_bench_exists(self):
        path = os.path.join(_GO_BENCH_DIR, "ingestion_bench_test.go")
        assert os.path.isfile(path), "ingestion_bench_test.go missing"

    def test_ingestion_bench_has_benchmark_functions(self):
        path = os.path.join(_GO_BENCH_DIR, "ingestion_bench_test.go")
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        assert "func Benchmark" in content, (
            "ingestion_bench_test.go has no Benchmark functions"
        )

    def test_ingestion_bench_has_multiple_benchmarks(self):
        path = os.path.join(_GO_BENCH_DIR, "ingestion_bench_test.go")
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        count = content.count("func Benchmark")
        assert count >= 3, (
            f"Expected at least 3 benchmark functions, found {count}"
        )

    def test_ingestion_bench_reports_allocs(self):
        path = os.path.join(_GO_BENCH_DIR, "ingestion_bench_test.go")
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        assert "ReportAllocs" in content, (
            "Benchmark should call b.ReportAllocs()"
        )


class TestPythonProfilingScripts:
    def test_profile_query_exists(self):
        path = os.path.join(_PY_BENCH_DIR, "profile_query.py")
        assert os.path.isfile(path), "profile_query.py missing"

    def test_profile_query_has_main(self):
        path = os.path.join(_PY_BENCH_DIR, "profile_query.py")
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        assert "def main" in content, "profile_query.py missing main()"

    def test_profile_query_uses_cprofile(self):
        path = os.path.join(_PY_BENCH_DIR, "profile_query.py")
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        assert "cProfile" in content, "profile_query.py should use cProfile"

    def test_profile_query_measures_latency(self):
        path = os.path.join(_PY_BENCH_DIR, "profile_query.py")
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        assert "p99" in content or "p95" in content, (
            "profile_query.py should measure percentile latencies"
        )


class TestBenchmarkRunner:
    def test_run_script_exists(self):
        path = os.path.join(_BENCH_DIR, "run_benchmarks.sh")
        assert os.path.isfile(path), "run_benchmarks.sh missing"

    def test_run_script_is_executable(self):
        path = os.path.join(_BENCH_DIR, "run_benchmarks.sh")
        mode = os.stat(path).st_mode
        assert mode & stat.S_IXUSR, "run_benchmarks.sh should be executable"

    def test_run_script_invokes_go_bench(self):
        path = os.path.join(_BENCH_DIR, "run_benchmarks.sh")
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        assert "go test" in content, (
            "run_benchmarks.sh should invoke go test -bench"
        )

    def test_run_script_invokes_python_profiler(self):
        path = os.path.join(_BENCH_DIR, "run_benchmarks.sh")
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        assert "profile_query.py" in content, (
            "run_benchmarks.sh should invoke profile_query.py"
        )


class TestCapacityPlanningDoc:
    def test_capacity_planning_exists(self):
        path = os.path.join(_DOCS_DIR, "capacity-planning.md")
        assert os.path.isfile(path), "docs/capacity-planning.md missing"

    def test_capacity_planning_has_scale_sections(self):
        path = os.path.join(_DOCS_DIR, "capacity-planning.md")
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        for scale in ["1x", "10x", "100x"]:
            assert scale in content, (
                f"capacity-planning.md missing {scale} scale calculations"
            )

    def test_capacity_planning_covers_components(self):
        path = os.path.join(_DOCS_DIR, "capacity-planning.md")
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        for component in ["Orchestrator", "Neo4j", "Kafka", "Ingestion"]:
            assert component in content, (
                f"capacity-planning.md missing {component} section"
            )

    def test_capacity_planning_has_resource_numbers(self):
        path = os.path.join(_DOCS_DIR, "capacity-planning.md")
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        assert "CPU" in content and "Memory" in content, (
            "capacity-planning.md should include CPU and Memory specifications"
        )
