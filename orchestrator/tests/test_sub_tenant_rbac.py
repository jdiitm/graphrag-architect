from __future__ import annotations

import pytest

from orchestrator.app.access_control import CypherPermissionFilter, SecurityPrincipal
from orchestrator.app.extraction_models import (
    DatabaseNode,
    K8sDeploymentNode,
    KafkaTopicNode,
    ServiceNode,
)
from orchestrator.app.ontology import build_default_ontology


class TestNodeModelsHaveReadRoles:

    def test_service_node_has_read_roles_field(self) -> None:
        node = ServiceNode(
            id="svc-1", name="svc-1", language="go",
            framework="gin", opentelemetry_enabled=True,
            tenant_id="t1", read_roles=["dev", "admin"],
        )
        assert node.read_roles == ["dev", "admin"]

    def test_service_node_read_roles_defaults_empty(self) -> None:
        node = ServiceNode(
            id="svc-1", name="svc-1", language="go",
            framework="gin", opentelemetry_enabled=True,
            tenant_id="t1",
        )
        assert node.read_roles == []

    def test_database_node_has_read_roles(self) -> None:
        node = DatabaseNode(
            id="db-1", type="postgresql", tenant_id="t1",
            read_roles=["admin"],
        )
        assert node.read_roles == ["admin"]

    def test_kafka_topic_has_read_roles(self) -> None:
        node = KafkaTopicNode(
            name="events", partitions=6, retention_ms=604800000,
            tenant_id="t1", read_roles=["dev"],
        )
        assert node.read_roles == ["dev"]

    def test_k8s_deployment_has_read_roles(self) -> None:
        node = K8sDeploymentNode(
            id="deploy-1", namespace="prod", replicas=3,
            tenant_id="t1", read_roles=["ops"],
        )
        assert node.read_roles == ["ops"]


class TestRoleFilterInCypherPermissionFilter:

    def test_viewer_role_adds_read_roles_filter(self) -> None:
        principal = SecurityPrincipal(
            team="platform", namespace="default", role="viewer",
        )
        pf = CypherPermissionFilter(principal)
        clause, params = pf.node_filter("n")
        assert "$acl_role IN n.read_roles" in clause
        assert params["acl_role"] == "viewer"

    def test_admin_role_skips_read_roles_filter(self) -> None:
        principal = SecurityPrincipal(
            team="platform", namespace="default", role="admin",
        )
        pf = CypherPermissionFilter(principal)
        clause, params = pf.node_filter("n")
        assert clause == ""
        assert "acl_role" not in params

    def test_dev_role_adds_read_roles_filter(self) -> None:
        principal = SecurityPrincipal(
            team="backend", namespace="staging", role="dev",
        )
        pf = CypherPermissionFilter(principal)
        clause, params = pf.node_filter("n")
        assert "$acl_role IN n.read_roles" in clause
        assert params["acl_role"] == "dev"

    def test_anonymous_principal_no_role_filter(self) -> None:
        principal = SecurityPrincipal(
            team="*", namespace="*", role="anonymous",
        )
        pf = CypherPermissionFilter(principal, default_deny_untagged=False)
        clause, params = pf.node_filter("n")
        assert "acl_role" not in params


class TestOntologyIncludesReadRoles:

    def test_service_ontology_has_read_roles_property(self) -> None:
        ontology = build_default_ontology()
        svc_def = ontology.get_node_type("Service")
        assert "read_roles" in svc_def.properties

    def test_read_roles_in_acl_fields(self) -> None:
        ontology = build_default_ontology()
        svc_def = ontology.get_node_type("Service")
        assert "read_roles" in svc_def.acl_fields

    def test_all_node_types_have_read_roles(self) -> None:
        ontology = build_default_ontology()
        for label in ontology.all_node_labels():
            node_def = ontology.get_node_type(label)
            assert "read_roles" in node_def.properties, (
                f"{label} missing read_roles property"
            )
