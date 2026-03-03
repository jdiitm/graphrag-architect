from __future__ import annotations

import json
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
LOAD_DIR = REPO_ROOT / "tests" / "load"


class TestLoadTestDirectoryExists:

    def test_load_directory_exists(self) -> None:
        assert LOAD_DIR.is_dir(), "tests/load/ directory not found"


class TestK6ScriptsExist:

    def test_at_least_three_k6_scripts(self) -> None:
        js_files = list(LOAD_DIR.glob("*.js"))
        assert len(js_files) >= 3, (
            f"Expected at least 3 k6 scripts, found {len(js_files)}: "
            f"{[f.name for f in js_files]}"
        )

    def test_ingestion_script_exists(self) -> None:
        assert (LOAD_DIR / "ingestion.js").exists()

    def test_query_script_exists(self) -> None:
        assert (LOAD_DIR / "query.js").exists()

    def test_soak_script_exists(self) -> None:
        assert (LOAD_DIR / "soak.js").exists()


class TestIngestionScriptThresholds:

    @pytest.fixture()
    def ingestion_content(self) -> str:
        return (LOAD_DIR / "ingestion.js").read_text()

    def test_has_thresholds_block(self, ingestion_content: str) -> None:
        assert "thresholds" in ingestion_content

    def test_p50_under_500ms(self, ingestion_content: str) -> None:
        assert "p(50)" in ingestion_content
        assert "500" in ingestion_content

    def test_p99_under_2s(self, ingestion_content: str) -> None:
        assert "p(99)" in ingestion_content
        assert "2000" in ingestion_content

    def test_has_ramp_stages(self, ingestion_content: str) -> None:
        assert "stages" in ingestion_content


class TestQueryScriptThresholds:

    @pytest.fixture()
    def query_content(self) -> str:
        return (LOAD_DIR / "query.js").read_text()

    def test_has_thresholds_block(self, query_content: str) -> None:
        assert "thresholds" in query_content

    def test_vector_p99_under_500ms(self, query_content: str) -> None:
        assert "p(99)" in query_content
        assert "500" in query_content

    def test_graph_p99_under_3s(self, query_content: str) -> None:
        assert "3000" in query_content

    def test_has_post_query_endpoint(self, query_content: str) -> None:
        assert "/query" in query_content


class TestSoakTestProfile:

    @pytest.fixture()
    def soak_content(self) -> str:
        return (LOAD_DIR / "soak.js").read_text()

    def test_has_extended_duration(self, soak_content: str) -> None:
        assert "24h" in soak_content or "86400" in soak_content

    def test_has_concurrent_users(self, soak_content: str) -> None:
        assert "100" in soak_content

    def test_has_memory_check(self, soak_content: str) -> None:
        assert "trend" in soak_content.lower() or "counter" in soak_content.lower()


class TestConfigFile:

    def test_config_file_exists(self) -> None:
        assert (LOAD_DIR / "config.json").exists()

    def test_config_is_valid_json(self) -> None:
        data = json.loads((LOAD_DIR / "config.json").read_text())
        assert isinstance(data, dict)

    def test_config_has_environment_settings(self) -> None:
        data = json.loads((LOAD_DIR / "config.json").read_text())
        for env in ("dev", "staging", "prod"):
            assert env in data, f"Missing environment: {env}"

    def test_each_environment_has_base_url(self) -> None:
        data = json.loads((LOAD_DIR / "config.json").read_text())
        for env in ("dev", "staging", "prod"):
            assert "base_url" in data[env], f"{env} missing base_url"
