from __future__ import annotations

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
    NodeTypeDefinition,
    build_default_ontology,
    generate_merge_cypher,
    generate_unwind_cypher,
    generate_edge_unwind_cypher,
)
from orchestrator.app.neo4j_client import build_unwind_queries


class TestOntologyMergeKeysInUnwind:

    def test_service_unwind_includes_tenant_in_merge_clause(self) -> None:
        ontology = build_default_ontology()
        svc_def = ontology.get_node_type("Service")
        cypher = generate_unwind_cypher("Service", svc_def)
        assert "tenant_id: row.tenant_id" in cypher
        merge_start = cypher.index("MERGE")
        set_start = cypher.index("SET")
        merge_section = cypher[merge_start:set_start]
        assert "tenant_id: row.tenant_id" in merge_section

    def test_database_unwind_includes_tenant_in_merge_clause(self) -> None:
        ontology = build_default_ontology()
        db_def = ontology.get_node_type("Database")
        cypher = generate_unwind_cypher("Database", db_def)
        merge_start = cypher.index("MERGE")
        set_start = cypher.index("SET")
        merge_section = cypher[merge_start:set_start]
        assert "tenant_id: row.tenant_id" in merge_section

    def test_kafka_topic_unwind_includes_tenant_in_merge_clause(self) -> None:
        ontology = build_default_ontology()
        topic_def = ontology.get_node_type("KafkaTopic")
        cypher = generate_unwind_cypher("KafkaTopic", topic_def)
        merge_start = cypher.index("MERGE")
        set_start = cypher.index("SET")
        merge_section = cypher[merge_start:set_start]
        assert "name: row.name" in merge_section
        assert "tenant_id: row.tenant_id" in merge_section

    def test_merge_keys_excluded_from_set_clause(self) -> None:
        ontology = build_default_ontology()
        svc_def = ontology.get_node_type("Service")
        cypher = generate_unwind_cypher("Service", svc_def)
        set_start = cypher.index("SET")
        set_clause = cypher[set_start:]
        assert "n.id = row.id" not in set_clause
        assert "n.tenant_id = row.tenant_id" not in set_clause
        assert "n.name = row.name" in set_clause

    def test_empty_merge_keys_falls_back_to_unique_key(self) -> None:
        node_def = NodeTypeDefinition(
            properties={"id": "string", "name": "string"},
            unique_key="id",
        )
        cypher = generate_unwind_cypher("TestNode", node_def)
        assert "MERGE (n:TestNode {id: row.id})" in cypher


class TestOntologyMergeKeysInSingleMerge:

    def test_single_merge_includes_tenant_in_merge_clause(self) -> None:
        ontology = build_default_ontology()
        svc_def = ontology.get_node_type("Service")
        cypher = generate_merge_cypher("Service", svc_def)
        merge_start = cypher.index("MERGE")
        set_start = cypher.index("SET")
        merge_section = cypher[merge_start:set_start]
        assert "tenant_id: $tenant_id" in merge_section

    def test_single_merge_excludes_merge_keys_from_set(self) -> None:
        ontology = build_default_ontology()
        svc_def = ontology.get_node_type("Service")
        cypher = generate_merge_cypher("Service", svc_def)
        set_start = cypher.index("SET")
        set_clause = cypher[set_start:]
        assert "n.id = $id" not in set_clause
        assert "n.tenant_id = $tenant_id" not in set_clause


class TestBuildUnwindQueriesFromOntology:

    def test_returns_queries_for_all_node_types(self) -> None:
        queries = build_unwind_queries()
        assert ServiceNode in queries
        assert DatabaseNode in queries
        assert KafkaTopicNode in queries
        assert K8sDeploymentNode in queries

    def test_returns_queries_for_all_edge_types(self) -> None:
        queries = build_unwind_queries()
        assert CallsEdge in queries
        assert ProducesEdge in queries
        assert ConsumesEdge in queries
        assert DeployedInEdge in queries

    def test_service_query_has_tenant_in_merge(self) -> None:
        queries = build_unwind_queries()
        cypher = queries[ServiceNode]
        assert "UNWIND $batch AS row" in cypher
        merge_start = cypher.index("MERGE")
        set_start = cypher.index("SET")
        merge_section = cypher[merge_start:set_start]
        assert "tenant_id: row.tenant_id" in merge_section

    def test_edge_query_has_tenant_in_match(self) -> None:
        queries = build_unwind_queries()
        cypher = queries[CallsEdge]
        assert "tenant_id: row.tenant_id" in cypher

    def test_generated_node_query_matches_hardcoded_semantics(self) -> None:
        queries = build_unwind_queries()
        svc_query = queries[ServiceNode]
        assert "UNWIND $batch AS row" in svc_query
        assert "n:Service" in svc_query
        assert "n.name = row.name" in svc_query
        assert "n.language = row.language" in svc_query
        assert "n.framework = row.framework" in svc_query
