import os
import re

import yaml


_INFRA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "infrastructure", "k8s",
)


def _load_all_yaml_docs(filename: str) -> list:
    filepath = os.path.join(_INFRA_DIR, filename)
    with open(filepath, encoding="utf-8") as fh:
        return list(yaml.safe_load_all(fh))


def _parse_memory_gi(value: str) -> float:
    match = re.match(r"^(\d+(?:\.\d+)?)\s*Gi$", value)
    if match:
        return float(match.group(1))
    match = re.match(r"^(\d+(?:\.\d+)?)\s*Mi$", value)
    if match:
        return float(match.group(1)) / 1024
    return 0.0


def _parse_memory_g(value: str) -> float:
    match = re.match(r"^(\d+(?:\.\d+)?)\s*g$", value)
    if match:
        return float(match.group(1))
    match = re.match(r"^(\d+(?:\.\d+)?)\s*m$", value)
    if match:
        return float(match.group(1)) / 1024
    return 0.0


class TestNeo4jStatefulSetResources:
    def _get_statefulset(self):
        docs = _load_all_yaml_docs("neo4j-statefulset.yaml")
        for doc in docs:
            if doc and doc.get("kind") == "StatefulSet":
                return doc
        raise AssertionError("No StatefulSet found")

    def _get_container(self):
        ss = self._get_statefulset()
        containers = ss["spec"]["template"]["spec"]["containers"]
        return containers[0]

    def test_memory_limit_at_least_16gi(self):
        container = self._get_container()
        mem_limit = container["resources"]["limits"]["memory"]
        assert _parse_memory_gi(mem_limit) >= 16.0, (
            f"Neo4j memory limit {mem_limit} is below 16Gi"
        )

    def test_memory_request_at_least_8gi(self):
        container = self._get_container()
        mem_request = container["resources"]["requests"]["memory"]
        assert _parse_memory_gi(mem_request) >= 8.0, (
            f"Neo4j memory request {mem_request} is below 8Gi"
        )

    def test_heap_max_at_least_4g(self):
        container = self._get_container()
        env_vars = {e["name"]: e["value"] for e in container.get("env", []) if "value" in e}
        heap_max = env_vars.get("NEO4J_dbms_memory_heap_max__size", "0m")
        assert _parse_memory_g(heap_max) >= 4.0, (
            f"Neo4j heap max {heap_max} is below 4g"
        )

    def test_pagecache_at_least_8g(self):
        container = self._get_container()
        env_vars = {e["name"]: e["value"] for e in container.get("env", []) if "value" in e}
        pagecache = env_vars.get("NEO4J_dbms_memory_pagecache_size", "0m")
        assert _parse_memory_g(pagecache) >= 8.0, (
            f"Neo4j pagecache {pagecache} is below 8g"
        )

    def test_off_heap_max_configured(self):
        container = self._get_container()
        env_names = {e["name"] for e in container.get("env", [])}
        assert "NEO4J_dbms_memory_off__heap_max__size" in env_names


class TestOrchestratorHPAMetrics:
    def _get_orchestrator_hpa(self):
        docs = _load_all_yaml_docs("hpa.yaml")
        for doc in docs:
            if doc and doc.get("metadata", {}).get("name") == "orchestrator-hpa":
                return doc
        raise AssertionError("No orchestrator-hpa found")

    def test_has_external_latency_metric(self):
        hpa = self._get_orchestrator_hpa()
        metrics = hpa["spec"]["metrics"]
        external_metrics = [
            m for m in metrics
            if m["type"] == "External"
        ]
        assert external_metrics, "orchestrator-hpa has no External metric"

    def test_external_metric_has_target_value(self):
        hpa = self._get_orchestrator_hpa()
        metrics = hpa["spec"]["metrics"]
        for metric in metrics:
            if metric["type"] == "External":
                target = metric["external"]["target"]
                assert "value" in target or "averageValue" in target
                return
        raise AssertionError("No External metric found")
