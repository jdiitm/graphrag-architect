import pytest

from orchestrator.app.access_control import (
    CypherPermissionFilter,
    PermissionMetadata,
    SecurityPrincipal,
)


class TestSecurityPrincipal:
    def test_from_header_parses_valid_token(self):
        principal = SecurityPrincipal.from_header(
            "Bearer team=platform,namespace=production,role=admin"
        )
        assert principal.team == "platform"
        assert principal.namespace == "production"
        assert principal.role == "admin"

    def test_from_header_defaults_on_missing_fields(self):
        principal = SecurityPrincipal.from_header(
            "Bearer team=platform"
        )
        assert principal.team == "platform"
        assert principal.namespace == "*"
        assert principal.role == "viewer"

    def test_from_header_returns_anonymous_on_empty(self):
        principal = SecurityPrincipal.from_header("")
        assert principal.team == "*"
        assert principal.namespace == "*"
        assert principal.role == "anonymous"

    def test_from_header_returns_anonymous_on_none(self):
        principal = SecurityPrincipal.from_header(None)
        assert principal.team == "*"
        assert principal.namespace == "*"
        assert principal.role == "anonymous"

    def test_is_admin(self):
        admin = SecurityPrincipal(team="platform", namespace="*", role="admin")
        assert admin.is_admin is True

    def test_non_admin(self):
        viewer = SecurityPrincipal(team="platform", namespace="production", role="viewer")
        assert viewer.is_admin is False


class TestPermissionMetadata:
    def test_create_with_all_fields(self):
        meta = PermissionMetadata(
            team_owner="platform",
            namespace_acl=["production", "staging"],
        )
        assert meta.team_owner == "platform"
        assert meta.namespace_acl == ["production", "staging"]

    def test_default_namespace_acl(self):
        meta = PermissionMetadata(team_owner="data-team")
        assert meta.namespace_acl == []


class TestCypherPermissionFilter:
    def test_admin_gets_no_filter(self):
        principal = SecurityPrincipal(team="platform", namespace="*", role="admin")
        filt = CypherPermissionFilter(principal)
        clause, params = filt.node_filter("n")
        assert clause == ""
        assert params == {}

    def test_team_scoped_filter(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        clause, params = filt.node_filter("n")
        assert "$acl_team" in clause or "$acl_namespace" in clause
        assert "platform" in params.values()

    def test_namespace_filter_restricts_nodes(self):
        principal = SecurityPrincipal(
            team="data-team", namespace="staging", role="editor"
        )
        filt = CypherPermissionFilter(principal)
        clause, params = filt.node_filter("n")
        assert "staging" in str(params.values())

    def test_anonymous_gets_public_only(self):
        principal = SecurityPrincipal(team="*", namespace="*", role="anonymous")
        filt = CypherPermissionFilter(principal)
        clause, params = filt.node_filter("n")
        assert clause != ""
        assert "public" in str(params.values()).lower() or "$acl_team" in clause

    def test_edge_filter_restricts_traversal(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        clause, params = filt.edge_filter("n", "m")
        assert clause != ""
        assert "$acl_team" in clause or "$acl_namespace" in clause

    def test_edge_filter_admin_no_restriction(self):
        principal = SecurityPrincipal(team="platform", namespace="*", role="admin")
        filt = CypherPermissionFilter(principal)
        clause, params = filt.edge_filter("n", "m")
        assert clause == ""
        assert params == {}

    def test_inject_into_cypher_adds_where(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        original = "MATCH (n:Service) RETURN n"
        filtered, params = filt.inject_into_cypher(original)
        assert "WHERE" in filtered or "AND" in filtered
        assert params

    def test_inject_preserves_existing_where(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        original = "MATCH (n:Service) WHERE n.language = 'Go' RETURN n"
        filtered, params = filt.inject_into_cypher(original)
        assert "n.language = 'Go'" in filtered
        assert params

    def test_inject_admin_returns_original(self):
        principal = SecurityPrincipal(team="ops", namespace="*", role="admin")
        filt = CypherPermissionFilter(principal)
        original = "MATCH (n:Service) RETURN n"
        filtered, params = filt.inject_into_cypher(original)
        assert filtered == original
        assert params == {}


class TestNodeMetadataOnIngestion:
    def test_service_cypher_includes_permission_fields(self):
        from orchestrator.app.neo4j_client import _service_cypher
        from orchestrator.app.extraction_models import ServiceNode

        node = ServiceNode(
            id="auth-svc",
            name="auth-service",
            language="Go",
            framework="gin",
            opentelemetry_enabled=True,
            team_owner="platform",
            namespace_acl=["production", "staging"],
        )
        cypher, params = _service_cypher(node)
        assert "team_owner" in cypher
        assert "namespace_acl" in cypher
        assert params["team_owner"] == "platform"
        assert params["namespace_acl"] == ["production", "staging"]

    def test_k8s_deployment_includes_namespace_acl(self):
        from orchestrator.app.neo4j_client import _k8s_deployment_cypher
        from orchestrator.app.extraction_models import K8sDeploymentNode

        node = K8sDeploymentNode(
            id="auth-deploy",
            namespace="production",
            replicas=3,
            team_owner="platform",
            namespace_acl=["production"],
        )
        cypher, params = _k8s_deployment_cypher(node)
        assert "team_owner" in cypher
        assert params["team_owner"] == "platform"
