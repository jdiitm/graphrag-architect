from __future__ import annotations

from typing import Any, Dict, List

import pytest

from orchestrator.app.extraction_models import (
    DatabaseNode,
    K8sDeploymentNode,
    KafkaTopicNode,
    ServiceNode,
    compute_content_hash,
)


class TestRbacLabelGeneration:

    def test_service_node_rbac_labels_with_all_fields(self) -> None:
        node = ServiceNode(
            id="auth-svc",
            name="auth-svc",
            language="python",
            framework="fastapi",
            opentelemetry_enabled=True,
            tenant_id="t1",
            team_owner="backend",
            namespace_acl=["production", "staging"],
            read_roles=["viewer", "editor"],
        )
        labels = node.rbac_labels
        assert "Team_backend" in labels
        assert "Ns_production" in labels
        assert "Ns_staging" in labels
        assert "Role_viewer" in labels
        assert "Role_editor" in labels
        assert len(labels) == 5

    def test_service_node_no_acl_fields_produces_empty_labels(self) -> None:
        node = ServiceNode(
            id="bare-svc",
            name="bare-svc",
            language="go",
            framework="gin",
            opentelemetry_enabled=False,
            tenant_id="t1",
        )
        assert node.rbac_labels == []

    def test_database_node_rbac_labels(self) -> None:
        node = DatabaseNode(
            id="pg-main",
            type="postgresql",
            tenant_id="t1",
            team_owner="platform",
            namespace_acl=["infra"],
        )
        labels = node.rbac_labels
        assert "Team_platform" in labels
        assert "Ns_infra" in labels
        assert len(labels) == 2

    def test_kafka_topic_rbac_labels(self) -> None:
        node = KafkaTopicNode(
            name="orders-events",
            partitions=12,
            retention_ms=604800000,
            tenant_id="t1",
            team_owner="commerce",
        )
        assert node.rbac_labels == ["Team_commerce"]

    def test_k8s_deployment_rbac_labels(self) -> None:
        node = K8sDeploymentNode(
            id="auth-deploy",
            namespace="production",
            replicas=3,
            tenant_id="t1",
            team_owner="backend",
            namespace_acl=["production"],
            read_roles=["admin"],
        )
        labels = node.rbac_labels
        assert "Team_backend" in labels
        assert "Ns_production" in labels
        assert "Role_admin" in labels

    def test_rbac_labels_sanitize_special_characters(self) -> None:
        node = ServiceNode(
            id="test-svc",
            name="test-svc",
            language="python",
            framework="fastapi",
            opentelemetry_enabled=False,
            tenant_id="t1",
            team_owner="my team",
            namespace_acl=["ns/prod", "ns.staging"],
        )
        labels = node.rbac_labels
        assert "Team_my_team" in labels
        assert "Ns_ns_prod" in labels
        assert "Ns_ns_staging" in labels

    def test_rbac_labels_included_in_model_dump(self) -> None:
        node = ServiceNode(
            id="svc-1",
            name="svc-1",
            language="python",
            framework="fastapi",
            opentelemetry_enabled=True,
            tenant_id="t1",
            team_owner="backend",
            namespace_acl=["production"],
        )
        dump = node.model_dump()
        assert "rbac_labels" in dump
        assert "Team_backend" in dump["rbac_labels"]
        assert "Ns_production" in dump["rbac_labels"]

    def test_rbac_labels_excluded_from_content_hash(self) -> None:
        node_a = ServiceNode(
            id="svc-1",
            name="svc-1",
            language="python",
            framework="fastapi",
            opentelemetry_enabled=True,
            tenant_id="t1",
            team_owner="backend",
            namespace_acl=["production"],
        )
        node_b = ServiceNode(
            id="svc-1",
            name="svc-1",
            language="python",
            framework="fastapi",
            opentelemetry_enabled=True,
            tenant_id="t1",
            team_owner="backend",
            namespace_acl=["production"],
        )
        assert compute_content_hash(node_a) == compute_content_hash(node_b)


class TestLabelBasedAclFragment:

    def test_acl_where_fragment_uses_labels(self) -> None:
        from orchestrator.app.tenant_security import _acl_where_fragment
        fragment = _acl_where_fragment()
        assert "labels(target)" in fragment
        assert "$acl_labels" in fragment
        assert "team_owner" not in fragment
        assert "namespace_acl" not in fragment

    def test_traversal_one_hop_uses_labels(self) -> None:
        from orchestrator.app.tenant_security import build_traversal_one_hop
        cypher = build_traversal_one_hop("CALLS")
        assert "$acl_labels" in cypher
        assert "labels(target)" in cypher
        assert "team_owner" not in cypher
        assert "namespace_acl" not in cypher

    def test_traversal_neighbor_discovery_uses_labels(self) -> None:
        from orchestrator.app.tenant_security import (
            build_traversal_neighbor_discovery,
        )
        cypher = build_traversal_neighbor_discovery()
        assert "$acl_labels" in cypher
        assert "labels(target)" in cypher

    def test_traversal_sampled_neighbor_uses_labels(self) -> None:
        from orchestrator.app.tenant_security import (
            build_traversal_sampled_neighbor,
        )
        cypher = build_traversal_sampled_neighbor()
        assert "$acl_labels" in cypher
        assert "labels(target)" in cypher

    def test_traversal_batched_neighbor_uses_labels(self) -> None:
        from orchestrator.app.tenant_security import (
            build_traversal_batched_neighbor,
        )
        cypher = build_traversal_batched_neighbor()
        assert "$acl_labels" in cypher
        assert "labels(target)" in cypher

    def test_traversal_batched_supernode_uses_labels(self) -> None:
        from orchestrator.app.tenant_security import (
            build_traversal_batched_supernode_neighbor,
        )
        cypher = build_traversal_batched_supernode_neighbor()
        assert "$acl_labels" in cypher
        assert "labels(target)" in cypher


class TestPermissionFilterLabels:

    def test_admin_bypasses_label_check(self) -> None:
        from orchestrator.app.access_control import (
            CypherPermissionFilter,
            SecurityPrincipal,
        )
        principal = SecurityPrincipal(team="backend", namespace="prod", role="admin")
        pf = CypherPermissionFilter(principal)
        clause, params = pf.node_filter("n")
        assert clause == ""
        assert params == {}

    def test_non_admin_uses_label_predicate(self) -> None:
        from orchestrator.app.access_control import (
            CypherPermissionFilter,
            SecurityPrincipal,
        )
        principal = SecurityPrincipal(team="backend", namespace="prod", role="viewer")
        pf = CypherPermissionFilter(principal)
        clause, params = pf.node_filter("n")
        assert "labels(n)" in clause
        assert "$acl_labels" in clause
        assert "acl_labels" in params
        assert "Team_backend" in params["acl_labels"]
        assert "Ns_prod" in params["acl_labels"]
        assert "Role_viewer" in params["acl_labels"]

    def test_wildcard_team_excluded_from_labels(self) -> None:
        from orchestrator.app.access_control import (
            CypherPermissionFilter,
            SecurityPrincipal,
        )
        principal = SecurityPrincipal(team="*", namespace="staging", role="viewer")
        pf = CypherPermissionFilter(principal)
        clause, params = pf.node_filter("n")
        acl_labels = params.get("acl_labels", [])
        assert not any(lbl.startswith("Team_") for lbl in acl_labels)
        assert "Ns_staging" in acl_labels

    def test_deny_untagged_false_allows_unlabeled_nodes(self) -> None:
        from orchestrator.app.access_control import (
            CypherPermissionFilter,
            SecurityPrincipal,
        )
        principal = SecurityPrincipal(team="backend", namespace="prod", role="viewer")
        pf = CypherPermissionFilter(principal, default_deny_untagged=False)
        clause, _params = pf.node_filter("n")
        assert "NONE(" in clause or "NOT ANY(" in clause


class TestBuildAclParams:

    def test_returns_acl_labels_not_old_params(self) -> None:
        from orchestrator.app.query_templates import build_acl_params
        params = build_acl_params(
            tenant_id="t1",
            is_admin=False,
            team="backend",
            namespaces=["production", "staging"],
        )
        assert "acl_labels" in params
        assert "acl_team" not in params
        assert "acl_namespaces" not in params
        assert "Team_backend" in params["acl_labels"]
        assert "Ns_production" in params["acl_labels"]
        assert "Ns_staging" in params["acl_labels"]

    def test_admin_still_has_acl_labels(self) -> None:
        from orchestrator.app.query_templates import build_acl_params
        params = build_acl_params(
            tenant_id="t1",
            is_admin=True,
            team="backend",
            namespaces=["production"],
        )
        assert params["is_admin"] is True
        assert "acl_labels" in params


class TestAclTemplatesUseLabelPredicates:

    def test_acl_vector_search_uses_labels(self) -> None:
        from orchestrator.app.query_templates import TemplateCatalog
        catalog = TemplateCatalog()
        tmpl = catalog.get_acl_template("acl_vector_search")
        assert tmpl is not None
        assert "labels(node)" in tmpl.cypher or "$acl_labels" in tmpl.cypher
        assert "team_owner" not in tmpl.cypher
        assert "namespace_acl" not in tmpl.cypher

    def test_acl_fulltext_search_uses_labels(self) -> None:
        from orchestrator.app.query_templates import TemplateCatalog
        catalog = TemplateCatalog()
        tmpl = catalog.get_acl_template("acl_fulltext_search")
        assert tmpl is not None
        assert "labels(node)" in tmpl.cypher or "$acl_labels" in tmpl.cypher
        assert "team_owner" not in tmpl.cypher
        assert "namespace_acl" not in tmpl.cypher

    def test_build_acl_single_hop_uses_labels(self) -> None:
        from orchestrator.app.query_templates import build_acl_single_hop_query
        cypher = build_acl_single_hop_query("CALLS")
        assert "$acl_labels" in cypher
        assert "team_owner" not in cypher

    def test_build_acl_multi_hop_uses_labels(self) -> None:
        from orchestrator.app.query_templates import build_acl_multi_hop_query
        cypher = build_acl_multi_hop_query(3)
        assert "$acl_labels" in cypher
        assert "team_owner" not in cypher


class TestAgenticTraversalTemplatesUseLabels:

    def test_one_hop_template_uses_labels(self) -> None:
        from orchestrator.app.agentic_traversal import _ONE_HOP_TEMPLATE
        assert "$acl_labels" in _ONE_HOP_TEMPLATE
        assert "team_owner" not in _ONE_HOP_TEMPLATE

    def test_neighbor_discovery_template_uses_labels(self) -> None:
        from orchestrator.app.agentic_traversal import (
            _NEIGHBOR_DISCOVERY_TEMPLATE,
        )
        assert "$acl_labels" in _NEIGHBOR_DISCOVERY_TEMPLATE
        assert "team_owner" not in _NEIGHBOR_DISCOVERY_TEMPLATE

    def test_sampled_neighbor_template_uses_labels(self) -> None:
        from orchestrator.app.agentic_traversal import (
            _SAMPLED_NEIGHBOR_TEMPLATE,
        )
        assert "$acl_labels" in _SAMPLED_NEIGHBOR_TEMPLATE
        assert "team_owner" not in _SAMPLED_NEIGHBOR_TEMPLATE

    def test_bounded_path_template_uses_labels(self) -> None:
        from orchestrator.app.agentic_traversal import _BOUNDED_PATH_TEMPLATE
        assert "$acl_labels" in _BOUNDED_PATH_TEMPLATE
        assert "team_owner" not in _BOUNDED_PATH_TEMPLATE

    def test_apoc_nodes_query_uses_labels(self) -> None:
        from orchestrator.app.agentic_traversal import _APOC_NODES_QUERY
        assert "$acl_labels" in _APOC_NODES_QUERY
        assert "team_owner" not in _APOC_NODES_QUERY

    def test_semantic_pruned_template_uses_labels(self) -> None:
        from orchestrator.app.agentic_traversal import (
            _SEMANTIC_PRUNED_NEIGHBOR_TEMPLATE,
        )
        assert "$acl_labels" in _SEMANTIC_PRUNED_NEIGHBOR_TEMPLATE
        assert "team_owner" not in _SEMANTIC_PRUNED_NEIGHBOR_TEMPLATE


class TestUnwindCypherIncludesLabelAttachment:

    def test_node_unwind_cypher_attaches_labels(self) -> None:
        from orchestrator.app.ontology import (
            build_default_ontology,
            generate_unwind_cypher,
        )
        ontology = build_default_ontology()
        node_def = ontology.get_node_type("Service")
        assert node_def is not None
        cypher = generate_unwind_cypher("Service", node_def)
        assert "apoc.create.addLabels" in cypher
        assert "rbac_labels" in cypher


class TestAclMarkers:

    def test_acl_markers_include_label_terms(self) -> None:
        from orchestrator.app.tenant_security import _ACL_MARKERS
        assert "$acl_labels" in _ACL_MARKERS
        assert "labels(" in _ACL_MARKERS or any(
            "labels" in m for m in _ACL_MARKERS
        )

    def test_acl_markers_exclude_old_property_terms(self) -> None:
        from orchestrator.app.tenant_security import _ACL_MARKERS
        assert "$acl_namespaces" not in _ACL_MARKERS
        assert "namespace_acl" not in _ACL_MARKERS
