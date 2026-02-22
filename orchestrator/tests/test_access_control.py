import hashlib
import hmac
import time

import pytest

from orchestrator.app.access_control import (
    CypherPermissionFilter,
    InvalidTokenError,
    SecurityPrincipal,
    sign_token,
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
        assert "$acl_team" in clause
        assert "$acl_namespace" in clause
        assert params["acl_team"] == "platform"
        assert params["acl_namespace"] == "production"

    def test_namespace_filter_restricts_nodes(self):
        principal = SecurityPrincipal(
            team="data-team", namespace="staging", role="editor"
        )
        filt = CypherPermissionFilter(principal)
        clause, params = filt.node_filter("n")
        assert params["acl_namespace"] == "staging"
        assert params["acl_team"] == "data-team"

    def test_anonymous_gets_public_only(self):
        principal = SecurityPrincipal(team="*", namespace="*", role="anonymous")
        filt = CypherPermissionFilter(principal)
        clause, params = filt.node_filter("n")
        assert clause != ""
        assert params["acl_team"] == "public"

    def test_wildcard_namespace_only_team_filtered(self):
        principal = SecurityPrincipal(
            team="platform", namespace="*", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        clause, params = filt.node_filter("n")
        assert "$acl_team" in clause
        assert "$acl_namespace" not in clause
        assert params["acl_team"] == "platform"
        assert "acl_namespace" not in params

    def test_wildcard_team_only_namespace_filtered(self):
        principal = SecurityPrincipal(
            team="*", namespace="staging", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        clause, params = filt.node_filter("n")
        assert "$acl_namespace" in clause
        assert "$acl_team" not in clause
        assert params["acl_namespace"] == "staging"
        assert "acl_team" not in params

    def test_edge_filter_restricts_traversal(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        clause, params = filt.edge_filter("m")
        assert clause != ""
        assert "$acl_team" in clause
        assert "$acl_namespace" in clause
        assert params["acl_team"] == "platform"
        assert params["acl_namespace"] == "production"

    def test_edge_filter_admin_no_restriction(self):
        principal = SecurityPrincipal(team="platform", namespace="*", role="admin")
        filt = CypherPermissionFilter(principal)
        clause, params = filt.edge_filter("m")
        assert clause == ""
        assert params == {}

    def test_inject_into_cypher_adds_where(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        original = "MATCH (n:Service) RETURN n"
        filtered, params = filt.inject_into_cypher(original)
        assert "WHERE" in filtered
        assert "$acl_team" in filtered
        assert "$acl_namespace" in filtered
        assert params["acl_team"] == "platform"
        assert params["acl_namespace"] == "production"

    def test_inject_preserves_existing_where(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        original = "MATCH (n:Service) WHERE n.language = 'Go' RETURN n"
        filtered, params = filt.inject_into_cypher(original)
        assert "n.language = 'Go'" in filtered
        assert params

    def test_inject_into_cypher_no_where_no_return(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        original = "MATCH (n:Service)"
        filtered, params = filt.inject_into_cypher(original)
        assert filtered.startswith("MATCH (n:Service) WHERE")
        assert "$acl_team" in filtered
        assert "$acl_namespace" in filtered
        assert params["acl_team"] == "platform"
        assert params["acl_namespace"] == "production"

    def test_inject_parenthesizes_existing_where_conditions(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        original = "MATCH (n:Service) WHERE n.active = true OR n.name = 'auth' RETURN n"
        filtered, params = filt.inject_into_cypher(original)
        assert "(n.active = true OR n.name = 'auth')" in filtered
        assert params["acl_team"] == "platform"

    def test_inject_into_cypher_custom_alias(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        original = "MATCH (s:Service) RETURN s"
        filtered, params = filt.inject_into_cypher(original, alias="s")
        assert "s.team_owner" in filtered
        assert "s.namespace_acl" in filtered
        assert "n.team_owner" not in filtered
        assert params["acl_team"] == "platform"

    def test_inject_admin_returns_original(self):
        principal = SecurityPrincipal(team="ops", namespace="*", role="admin")
        filt = CypherPermissionFilter(principal)
        original = "MATCH (n:Service) RETURN n"
        filtered, params = filt.inject_into_cypher(original)
        assert filtered == original
        assert params == {}


class TestSignToken:
    def test_produces_payload_dot_signature_format(self):
        payload = "team=ops,role=admin"
        secret = "test-secret-key"
        token = sign_token(payload, secret)
        assert "." in token
        parts = token.rsplit(".", 1)
        assert len(parts) == 2
        assert parts[0].startswith(payload)
        assert ",iat=" in parts[0]
        assert ",exp=" in parts[0]

    def test_signature_is_valid_hmac_sha256(self):
        payload = "team=ops,role=admin"
        secret = "test-secret-key"
        token = sign_token(payload, secret)
        full_payload, signature = token.rsplit(".", 1)
        expected = hmac.new(
            secret.encode(), full_payload.encode(), hashlib.sha256
        ).hexdigest()
        assert signature == expected

    def test_different_secrets_produce_different_signatures(self):
        payload = "team=ops,role=admin"
        token_a = sign_token(payload, "secret-a")
        token_b = sign_token(payload, "secret-b")
        assert token_a != token_b


class TestTokenExpiration:
    def test_sign_token_embeds_iat_and_exp(self):
        token = sign_token("team=ops,role=admin", "secret", ttl_seconds=3600)
        payload, _ = token.rsplit(".", 1)
        fields = dict(pair.split("=", 1) for pair in payload.split(",") if "=" in pair)
        assert "iat" in fields
        assert "exp" in fields
        iat = int(fields["iat"])
        exp = int(fields["exp"])
        assert exp - iat == 3600
        assert abs(iat - int(time.time())) < 5

    def test_expired_token_raises_invalid_token_error(self):
        expired_payload = f"team=ops,role=admin,iat={int(time.time()) - 7200},exp={int(time.time()) - 3600}"
        sig = hmac.new(
            "secret".encode(), expired_payload.encode(), hashlib.sha256
        ).hexdigest()
        token = f"{expired_payload}.{sig}"
        with pytest.raises(InvalidTokenError, match="token expired"):
            SecurityPrincipal.from_header(
                f"Bearer {token}", token_secret="secret"
            )

    def test_token_within_ttl_validates_successfully(self):
        token = sign_token("team=ops,role=admin", "secret", ttl_seconds=3600)
        principal = SecurityPrincipal.from_header(
            f"Bearer {token}", token_secret="secret"
        )
        assert principal.team == "ops"
        assert principal.role == "admin"

    def test_token_at_exact_expiry_boundary_rejected(self):
        now = int(time.time())
        boundary_payload = f"team=ops,role=viewer,iat={now},exp={now}"
        sig = hmac.new(
            "secret".encode(), boundary_payload.encode(), hashlib.sha256
        ).hexdigest()
        token = f"{boundary_payload}.{sig}"
        with pytest.raises(InvalidTokenError, match="token expired"):
            SecurityPrincipal.from_header(
                f"Bearer {token}", token_secret="secret"
            )

    def test_default_ttl_is_one_hour(self):
        token = sign_token("team=ops,role=admin", "secret")
        payload, _ = token.rsplit(".", 1)
        fields = dict(pair.split("=", 1) for pair in payload.split(",") if "=" in pair)
        iat = int(fields["iat"])
        exp = int(fields["exp"])
        assert exp - iat == 3600


class TestHMACTokenVerification:
    def test_valid_signed_token_parses_correctly(self):
        secret = "production-secret"
        payload = "team=platform,namespace=production,role=admin"
        token = sign_token(payload, secret)
        principal = SecurityPrincipal.from_header(
            f"Bearer {token}", token_secret=secret
        )
        assert principal.team == "platform"
        assert principal.namespace == "production"
        assert principal.role == "admin"

    def test_invalid_signature_raises_error(self):
        secret = "real-secret"
        payload = "team=ops,role=admin"
        forged_token = f"{payload}.deadbeef0000"
        with pytest.raises(InvalidTokenError):
            SecurityPrincipal.from_header(
                f"Bearer {forged_token}", token_secret=secret
            )

    def test_tampered_payload_raises_error(self):
        secret = "real-secret"
        original_payload = "team=ops,role=viewer"
        token = sign_token(original_payload, secret)
        _, sig = token.rsplit(".", 1)
        tampered_token = f"team=ops,role=admin.{sig}"
        with pytest.raises(InvalidTokenError):
            SecurityPrincipal.from_header(
                f"Bearer {tampered_token}", token_secret=secret
            )

    def test_missing_signature_raises_error(self):
        secret = "real-secret"
        with pytest.raises(InvalidTokenError):
            SecurityPrincipal.from_header(
                "Bearer team=ops,role=admin", token_secret=secret
            )

    def test_empty_header_still_returns_anonymous(self):
        principal = SecurityPrincipal.from_header("", token_secret="some-secret")
        assert principal.role == "anonymous"
        assert principal.team == "*"

    def test_none_header_still_returns_anonymous(self):
        principal = SecurityPrincipal.from_header(None, token_secret="some-secret")
        assert principal.role == "anonymous"
        assert principal.team == "*"

    def test_no_secret_skips_verification(self):
        principal = SecurityPrincipal.from_header(
            "Bearer team=ops,role=admin", token_secret=""
        )
        assert principal.role == "admin"
        assert principal.team == "ops"

    def test_no_secret_default_skips_verification(self):
        principal = SecurityPrincipal.from_header(
            "Bearer team=ops,role=admin"
        )
        assert principal.role == "admin"
        assert principal.team == "ops"


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

    def test_database_cypher_includes_permission_fields(self):
        from orchestrator.app.neo4j_client import _database_cypher
        from orchestrator.app.extraction_models import DatabaseNode

        node = DatabaseNode(
            id="orders-db",
            type="PostgreSQL",
            team_owner="data-team",
            namespace_acl=["production", "staging"],
        )
        cypher, params = _database_cypher(node)
        assert "team_owner" in cypher
        assert "namespace_acl" in cypher
        assert params["team_owner"] == "data-team"
        assert params["namespace_acl"] == ["production", "staging"]

    def test_kafka_topic_cypher_includes_permission_fields(self):
        from orchestrator.app.neo4j_client import _kafka_topic_cypher
        from orchestrator.app.extraction_models import KafkaTopicNode

        node = KafkaTopicNode(
            name="user-events",
            partitions=12,
            retention_ms=604800000,
            team_owner="platform",
            namespace_acl=["production"],
        )
        cypher, params = _kafka_topic_cypher(node)
        assert "team_owner" in cypher
        assert "namespace_acl" in cypher
        assert params["team_owner"] == "platform"
        assert params["namespace_acl"] == ["production"]
