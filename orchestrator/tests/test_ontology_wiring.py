from __future__ import annotations

import os
import tempfile
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.extraction_models import (
    CallsEdge,
    ConsumesEdge,
    DatabaseNode,
    DeployedInEdge,
    K8sDeploymentNode,
    KafkaTopicNode,
    ProducesEdge,
    ServiceNode,
)
from orchestrator.app.ontology import (
    Ontology,
    OntologyLoader,
    build_default_ontology,
    generate_edge_merge_cypher,
    generate_merge_cypher,
    load_ontology,
)
from orchestrator.app.neo4j_client import (
    GraphRepository,
    build_cypher_dispatch,
    build_unwind_queries,
)


SAMPLE_YAML = """
node_types:
  Service:
    properties:
      id: string
      name: string
      language: string
      framework: string
      opentelemetry_enabled: bool
      tenant_id: string
      team_owner: string
      namespace_acl: list
      read_roles: list
      confidence: float
    unique_key: id
    merge_keys:
      - id
      - tenant_id
    acl_fields:
      - team_owner
      - namespace_acl
      - read_roles
  Database:
    properties:
      id: string
      type: string
      tenant_id: string
      team_owner: string
      namespace_acl: list
      read_roles: list
    unique_key: id
    merge_keys:
      - id
      - tenant_id
    acl_fields:
      - team_owner
      - namespace_acl
      - read_roles
  KafkaTopic:
    properties:
      name: string
      partitions: int
      retention_ms: int
      tenant_id: string
      team_owner: string
      namespace_acl: list
      read_roles: list
    unique_key: name
    merge_keys:
      - name
      - tenant_id
    acl_fields:
      - team_owner
      - namespace_acl
      - read_roles
  K8sDeployment:
    properties:
      id: string
      namespace: string
      replicas: int
      tenant_id: string
      team_owner: string
      namespace_acl: list
      read_roles: list
    unique_key: id
    merge_keys:
      - id
      - tenant_id
    acl_fields:
      - team_owner
      - namespace_acl
      - read_roles

edge_types:
  CALLS:
    source_type: Service
    target_type: Service
    properties:
      protocol: string
      confidence: float
      ingestion_id: string
      last_seen_at: string
    source_key: id
    source_param: source_service_id
    target_key: id
    target_param: target_service_id
    source_label: Service
    target_label: Service
  PRODUCES:
    source_type: Service
    target_type: KafkaTopic
    properties:
      event_schema: string
      ingestion_id: string
      last_seen_at: string
    source_key: id
    source_param: service_id
    target_key: name
    target_param: topic_name
    source_label: Service
    target_label: KafkaTopic
  CONSUMES:
    source_type: Service
    target_type: KafkaTopic
    properties:
      consumer_group: string
      ingestion_id: string
      last_seen_at: string
    source_key: id
    source_param: service_id
    target_key: name
    target_param: topic_name
    source_label: Service
    target_label: KafkaTopic
  DEPLOYED_IN:
    source_type: Service
    target_type: K8sDeployment
    properties:
      ingestion_id: string
      last_seen_at: string
    source_key: id
    source_param: service_id
    target_key: id
    target_param: deployment_id
    source_label: Service
    target_label: K8sDeployment
"""

SAMPLE_SERVICE = ServiceNode(
    id="order-service",
    name="order-service",
    language="go",
    framework="gin",
    opentelemetry_enabled=True,
    tenant_id="test-tenant",
)

SAMPLE_CALLS = CallsEdge(
    source_service_id="user-service",
    target_service_id="order-service",
    protocol="http",
    tenant_id="test-tenant",
)


class TestLoadOntology:

    def test_returns_default_when_no_env_var(self) -> None:
        env = os.environ.copy()
        env.pop("ONTOLOGY_FILE", None)
        with pytest.MonkeyPatch.context() as mp:
            mp.delenv("ONTOLOGY_FILE", raising=False)
            ontology = load_ontology()
        default = build_default_ontology()
        assert set(ontology.all_node_labels()) == set(default.all_node_labels())
        assert set(ontology.all_edge_types()) == set(default.all_edge_types())

    def test_reads_from_env_var(self) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False,
        ) as fh:
            fh.write(SAMPLE_YAML)
            fh.flush()
            path = fh.name
        try:
            with pytest.MonkeyPatch.context() as mp:
                mp.setenv("ONTOLOGY_FILE", path)
                ontology = load_ontology()
            assert "Service" in ontology.all_node_labels()
            assert "CALLS" in ontology.all_edge_types()
        finally:
            os.unlink(path)

    def test_explicit_path_overrides_env(self) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False,
        ) as fh:
            fh.write(SAMPLE_YAML)
            fh.flush()
            path = fh.name
        try:
            with pytest.MonkeyPatch.context() as mp:
                mp.setenv("ONTOLOGY_FILE", "/nonexistent/bad.yaml")
                ontology = load_ontology(path=path)
            assert "Service" in ontology.all_node_labels()
        finally:
            os.unlink(path)


class TestBuildUnwindQueriesAcceptsOntology:

    def test_explicit_ontology_used_instead_of_default(self) -> None:
        custom = OntologyLoader.from_yaml_string(SAMPLE_YAML)
        queries = build_unwind_queries(ontology=custom)
        assert ServiceNode in queries
        assert DatabaseNode in queries

    def test_none_falls_back_to_default(self) -> None:
        queries_default = build_unwind_queries()
        queries_none = build_unwind_queries(ontology=None)
        assert set(queries_default.keys()) == set(queries_none.keys())
        for key in queries_default:
            assert queries_default[key] == queries_none[key]


class TestBuildCypherDispatch:

    def test_covers_all_four_node_types(self) -> None:
        dispatch = build_cypher_dispatch()
        assert ServiceNode in dispatch
        assert DatabaseNode in dispatch
        assert KafkaTopicNode in dispatch
        assert K8sDeploymentNode in dispatch

    def test_covers_all_four_edge_types(self) -> None:
        dispatch = build_cypher_dispatch()
        assert CallsEdge in dispatch
        assert ProducesEdge in dispatch
        assert ConsumesEdge in dispatch
        assert DeployedInEdge in dispatch

    def test_service_dispatch_matches_ontology_merge(self) -> None:
        dispatch = build_cypher_dispatch()
        ontology = build_default_ontology()
        expected_query = generate_merge_cypher(
            "Service", ontology.get_node_type("Service"),
        )
        query, params = dispatch[ServiceNode](SAMPLE_SERVICE)
        assert query == expected_query
        assert params["id"] == "order-service"

    def test_calls_dispatch_matches_ontology_edge_merge(self) -> None:
        dispatch = build_cypher_dispatch()
        ontology = build_default_ontology()
        expected_query = generate_edge_merge_cypher(
            "CALLS", ontology.get_edge_type("CALLS"),
        )
        query, params = dispatch[CallsEdge](SAMPLE_CALLS)
        assert query == expected_query
        assert params["source_service_id"] == "user-service"

    def test_accepts_custom_ontology(self) -> None:
        custom = OntologyLoader.from_yaml_string(SAMPLE_YAML)
        dispatch = build_cypher_dispatch(ontology=custom)
        assert ServiceNode in dispatch
        assert CallsEdge in dispatch


class TestBackwardCompatNamedFunctions:

    def test_service_cypher_importable_and_callable(self) -> None:
        from orchestrator.app.neo4j_client import _service_cypher
        query, params = _service_cypher(SAMPLE_SERVICE)
        assert "MERGE" in query
        assert ":Service" in query
        assert params["id"] == "order-service"

    def test_calls_cypher_importable_and_callable(self) -> None:
        from orchestrator.app.neo4j_client import _calls_cypher
        query, params = _calls_cypher(SAMPLE_CALLS)
        assert "MATCH" in query
        assert ":CALLS" in query
        assert params["source_service_id"] == "user-service"

    def test_all_eight_named_functions_exist(self) -> None:
        from orchestrator.app.neo4j_client import (
            _service_cypher,
            _database_cypher,
            _kafka_topic_cypher,
            _k8s_deployment_cypher,
            _calls_cypher,
            _produces_cypher,
            _consumes_cypher,
            _deployed_in_cypher,
        )
        assert callable(_service_cypher)
        assert callable(_database_cypher)
        assert callable(_kafka_topic_cypher)
        assert callable(_k8s_deployment_cypher)
        assert callable(_calls_cypher)
        assert callable(_produces_cypher)
        assert callable(_consumes_cypher)
        assert callable(_deployed_in_cypher)


class TestGraphRepositoryAcceptsOntology:

    def test_init_accepts_ontology_parameter(self) -> None:
        driver = MagicMock()
        ontology = build_default_ontology()
        repo = GraphRepository(driver, ontology=ontology)
        assert repo._ontology is ontology

    def test_init_without_ontology_uses_default(self) -> None:
        driver = MagicMock()
        repo = GraphRepository(driver)
        default = build_default_ontology()
        assert set(repo._ontology.all_node_labels()) == set(
            default.all_node_labels()
        )

    @pytest.mark.asyncio
    async def test_custom_ontology_used_for_unwind_queries(self) -> None:
        custom = OntologyLoader.from_yaml_string(SAMPLE_YAML)
        mock_tx = AsyncMock()
        mock_tx.run = AsyncMock()

        async def _exec_write(fn: Any, **kw: Any) -> Any:
            return await fn(mock_tx, **kw)

        mock_session = AsyncMock()
        mock_session.execute_write = AsyncMock(side_effect=_exec_write)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        driver = MagicMock()
        driver.session.return_value = mock_session

        repo = GraphRepository(driver, ontology=custom)
        await repo.commit_topology([SAMPLE_SERVICE])

        unwind_calls = [
            c for c in mock_tx.run.call_args_list
            if "UNWIND" in c.args[0]
        ]
        assert len(unwind_calls) >= 1
        cypher = unwind_calls[0].args[0]
        assert ":Service" in cypher
