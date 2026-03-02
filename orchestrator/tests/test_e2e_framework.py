import ast
import pathlib
from typing import List, Set

import pytest

E2E_DIR = pathlib.Path(__file__).parent / "e2e"
PROJECT_ROOT = pathlib.Path(__file__).parent.parent.parent


def _parse_module(filepath: pathlib.Path) -> ast.Module:
    return ast.parse(filepath.read_text(encoding="utf-8"), filename=str(filepath))


def _collect_test_functions(filepath: pathlib.Path) -> List[str]:
    tree = _parse_module(filepath)
    return [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    ]


def _collect_fixture_names(filepath: pathlib.Path) -> Set[str]:
    tree = _parse_module(filepath)
    fixtures: Set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            decorator_name = ""
            if isinstance(decorator, ast.Name):
                decorator_name = decorator.id
            elif isinstance(decorator, ast.Attribute):
                decorator_name = decorator.attr
            elif isinstance(decorator, ast.Call):
                func = decorator.func
                if isinstance(func, ast.Name):
                    decorator_name = func.id
                elif isinstance(func, ast.Attribute):
                    decorator_name = func.attr
            if decorator_name == "fixture":
                fixtures.add(node.name)
    return fixtures


def _file_has_e2e_marker(filepath: pathlib.Path) -> bool:
    source = filepath.read_text(encoding="utf-8")
    return "pytest.mark.e2e" in source


def _read_pyproject_markers() -> str:
    pyproject = PROJECT_ROOT / "pyproject.toml"
    return pyproject.read_text(encoding="utf-8")


class TestE2EDirectoryStructure:
    def test_e2e_directory_exists(self):
        assert E2E_DIR.is_dir(), "orchestrator/tests/e2e/ directory must exist"

    def test_e2e_init_file_exists(self):
        init_file = E2E_DIR / "__init__.py"
        assert init_file.exists(), "e2e/__init__.py must exist"

    def test_conftest_exists(self):
        conftest = E2E_DIR / "conftest.py"
        assert conftest.exists(), "e2e/conftest.py must exist"

    def test_ingestion_pipeline_tests_exist(self):
        test_file = E2E_DIR / "test_ingestion_pipeline.py"
        assert test_file.exists()

    def test_query_pipeline_tests_exist(self):
        test_file = E2E_DIR / "test_query_pipeline.py"
        assert test_file.exists()

    def test_dlq_handling_tests_exist(self):
        test_file = E2E_DIR / "test_dlq_handling.py"
        assert test_file.exists()


class TestE2EConftest:
    def test_conftest_defines_neo4j_fixture(self):
        fixtures = _collect_fixture_names(E2E_DIR / "conftest.py")
        assert "neo4j_container" in fixtures

    def test_conftest_defines_kafka_fixture(self):
        fixtures = _collect_fixture_names(E2E_DIR / "conftest.py")
        assert "kafka_container" in fixtures

    def test_conftest_defines_orchestrator_client_fixture(self):
        fixtures = _collect_fixture_names(E2E_DIR / "conftest.py")
        assert "orchestrator_client" in fixtures

    def test_conftest_defines_env_fixture(self):
        fixtures = _collect_fixture_names(E2E_DIR / "conftest.py")
        assert "e2e_env" in fixtures

    def test_conftest_has_neo4j_image_version(self):
        source = (E2E_DIR / "conftest.py").read_text(encoding="utf-8")
        assert "neo4j:5.26" in source

    def test_conftest_has_graceful_skip(self):
        source = (E2E_DIR / "conftest.py").read_text(encoding="utf-8")
        assert "HAS_TESTCONTAINERS" in source
        assert "skipif" in source


class TestE2EMarkers:
    def test_ingestion_tests_have_e2e_marker(self):
        assert _file_has_e2e_marker(E2E_DIR / "test_ingestion_pipeline.py")

    def test_query_tests_have_e2e_marker(self):
        assert _file_has_e2e_marker(E2E_DIR / "test_query_pipeline.py")

    def test_dlq_tests_have_e2e_marker(self):
        assert _file_has_e2e_marker(E2E_DIR / "test_dlq_handling.py")

    def test_pyproject_has_e2e_marker_registered(self):
        content = _read_pyproject_markers()
        assert "e2e" in content
        assert "markers" in content or "mark" in content


class TestE2ETestCount:
    def test_at_least_ten_e2e_test_functions(self):
        total_tests: List[str] = []
        for test_file in E2E_DIR.glob("test_*.py"):
            total_tests.extend(_collect_test_functions(test_file))
        assert len(total_tests) >= 10, (
            f"Expected at least 10 E2E test functions, found {len(total_tests)}: "
            f"{total_tests}"
        )

    def test_ingestion_pipeline_has_five_tests(self):
        tests = _collect_test_functions(E2E_DIR / "test_ingestion_pipeline.py")
        assert len(tests) >= 5, (
            f"Expected >= 5 ingestion tests, found {len(tests)}: {tests}"
        )

    def test_query_pipeline_has_three_tests(self):
        tests = _collect_test_functions(E2E_DIR / "test_query_pipeline.py")
        assert len(tests) >= 3, (
            f"Expected >= 3 query tests, found {len(tests)}: {tests}"
        )

    def test_dlq_handling_has_two_tests(self):
        tests = _collect_test_functions(E2E_DIR / "test_dlq_handling.py")
        assert len(tests) >= 2, (
            f"Expected >= 2 DLQ tests, found {len(tests)}: {tests}"
        )


class TestE2ETestNames:
    def test_ingest_k8s_manifest_test_exists(self):
        tests = _collect_test_functions(E2E_DIR / "test_ingestion_pipeline.py")
        assert "test_ingest_kubernetes_manifest" in tests

    def test_ingest_source_code_test_exists(self):
        tests = _collect_test_functions(E2E_DIR / "test_ingestion_pipeline.py")
        assert "test_ingest_source_code" in tests

    def test_ingest_idempotent_test_exists(self):
        tests = _collect_test_functions(E2E_DIR / "test_ingestion_pipeline.py")
        assert "test_ingest_idempotent" in tests

    def test_ingest_invalid_payload_test_exists(self):
        tests = _collect_test_functions(E2E_DIR / "test_ingestion_pipeline.py")
        assert "test_ingest_invalid_payload" in tests

    def test_ingest_large_batch_test_exists(self):
        tests = _collect_test_functions(E2E_DIR / "test_ingestion_pipeline.py")
        assert "test_ingest_large_batch" in tests

    def test_query_after_ingest_test_exists(self):
        tests = _collect_test_functions(E2E_DIR / "test_query_pipeline.py")
        assert "test_query_after_ingest" in tests

    def test_query_empty_graph_test_exists(self):
        tests = _collect_test_functions(E2E_DIR / "test_query_pipeline.py")
        assert "test_query_empty_graph" in tests

    def test_query_acl_filtering_test_exists(self):
        tests = _collect_test_functions(E2E_DIR / "test_query_pipeline.py")
        assert "test_query_acl_filtering" in tests

    def test_dlq_neo4j_failure_test_exists(self):
        tests = _collect_test_functions(E2E_DIR / "test_dlq_handling.py")
        assert "test_ingest_triggers_dlq_on_neo4j_failure" in tests

    def test_circuit_breaker_test_exists(self):
        tests = _collect_test_functions(E2E_DIR / "test_dlq_handling.py")
        assert "test_circuit_breaker_opens" in tests
