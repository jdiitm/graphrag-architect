import time

import jwt
import pytest

from orchestrator.app.access_control import (
    CypherPermissionFilter,
    InvalidTokenError,
    JWT_ALGORITHM,
    SecurityPrincipal,
    sign_token,
)


class TestSecurityPrincipal:
    def test_from_header_parses_valid_jwt(self):
        secret = "test-key"
        token = sign_token(
            {"team": "platform", "namespace": "production", "role": "admin"},
            secret,
        )
        principal = SecurityPrincipal.from_header(
            f"Bearer {token}", token_secret=secret
        )
        assert principal.team == "platform"
        assert principal.namespace == "production"
        assert principal.role == "admin"

    def test_from_header_defaults_on_missing_fields(self):
        secret = "test-key"
        token = sign_token({"team": "platform"}, secret)
        principal = SecurityPrincipal.from_header(
            f"Bearer {token}", token_secret=secret
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

    def test_inject_skips_where_inside_nested_subquery(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        original = (
            "MATCH (n:Service) "
            "CALL { MATCH (m:Database) WHERE m.type = 'PostgreSQL' RETURN m } "
            "RETURN n, m"
        )
        filtered, params = filt.inject_into_cypher(original)
        assert params["acl_team"] == "platform"
        assert "m.type = 'PostgreSQL'" in filtered, (
            "Subquery WHERE condition must be preserved"
        )
        subquery_where = filtered.find("WHERE", filtered.find("CALL {"))
        outer_acl_pos = filtered.find("n.team_owner")
        subquery_end = filtered.find("}")
        assert outer_acl_pos > subquery_end, (
            "ACL clause must not be injected inside the subquery"
        )

    def test_inject_skips_where_inside_case_expression(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        original = (
            "MATCH (n:Service) "
            "RETURN CASE WHEN n.language = 'Go' THEN 'backend' ELSE 'other' END"
        )
        filtered, params = filt.inject_into_cypher(original)
        assert params["acl_team"] == "platform"
        assert "CASE WHEN" in filtered, "CASE expression must be preserved intact"
        where_pos = filtered.find("WHERE")
        case_pos = filtered.find("CASE")
        assert where_pos < case_pos, (
            "ACL WHERE must be before CASE expression"
        )

    def test_inject_skips_where_in_string_literal(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        original = (
            "MATCH (n:Service) WHERE n.name = 'WHERE_SERVICE' RETURN n"
        )
        filtered, params = filt.inject_into_cypher(original)
        assert params["acl_team"] == "platform"
        assert "'WHERE_SERVICE'" in filtered, (
            "String literal must be preserved"
        )

    def test_inject_skips_return_in_string_literal(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        original = (
            "MATCH (n:Service) WHERE n.description CONTAINS 'RETURN value' RETURN n"
        )
        filtered, params = filt.inject_into_cypher(original)
        assert params["acl_team"] == "platform"
        assert "'RETURN value'" in filtered, (
            "String literal containing RETURN must be preserved"
        )
        acl_clause_pos = filtered.find("n.team_owner")
        final_return_pos = filtered.rfind("RETURN n")
        assert acl_clause_pos < final_return_pos

    def test_inject_handles_multiple_match_clauses(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        original = (
            "MATCH (n:Service)-[:CALLS]->(m:Service) "
            "WHERE n.language = 'Go' "
            "RETURN n, m"
        )
        filtered, params = filt.inject_into_cypher(original)
        assert params["acl_team"] == "platform"
        assert "n.language = 'Go'" in filtered

    def test_inject_nested_braces_depth_tracking(self):
        principal = SecurityPrincipal(
            team="ops", namespace="staging", role="viewer"
        )
        filt = CypherPermissionFilter(principal)
        original = (
            "MATCH (n:Service) "
            "CALL { WITH n MATCH (n)-[:CALLS]->(t:Service) WHERE t.active = true RETURN count(t) AS cnt } "
            "WHERE n.language = 'Python' "
            "RETURN n, cnt"
        )
        filtered, params = filt.inject_into_cypher(original)
        assert params["acl_team"] == "ops"
        assert "t.active = true" in filtered
        assert "n.language = 'Python'" in filtered


class TestSignToken:
    def test_produces_valid_jwt(self):
        claims = {"team": "ops", "role": "admin"}
        secret = "test-secret-key"
        token = sign_token(claims, secret)
        decoded = jwt.decode(token, secret, algorithms=[JWT_ALGORITHM])
        assert decoded["team"] == "ops"
        assert decoded["role"] == "admin"
        assert "iat" in decoded
        assert "exp" in decoded

    def test_jwt_contains_standard_time_claims(self):
        claims = {"team": "ops", "role": "admin"}
        secret = "test-secret-key"
        token = sign_token(claims, secret, ttl_seconds=3600)
        decoded = jwt.decode(token, secret, algorithms=[JWT_ALGORITHM])
        assert decoded["exp"] - decoded["iat"] == 3600
        assert abs(decoded["iat"] - int(time.time())) < 5

    def test_different_secrets_produce_different_tokens(self):
        claims = {"team": "ops", "role": "admin"}
        token_a = sign_token(claims, "secret-a")
        token_b = sign_token(claims, "secret-b")
        assert token_a != token_b


class TestTokenExpiration:
    def test_sign_token_embeds_iat_and_exp(self):
        token = sign_token({"team": "ops", "role": "admin"}, "secret", ttl_seconds=3600)
        decoded = jwt.decode(token, "secret", algorithms=[JWT_ALGORITHM])
        assert "iat" in decoded
        assert "exp" in decoded
        assert decoded["exp"] - decoded["iat"] == 3600
        assert abs(decoded["iat"] - int(time.time())) < 5

    def test_expired_token_raises_invalid_token_error(self):
        expired_payload = {
            "team": "ops",
            "role": "admin",
            "iat": int(time.time()) - 7200,
            "exp": int(time.time()) - 3600,
        }
        token = jwt.encode(expired_payload, "secret", algorithm=JWT_ALGORITHM)
        with pytest.raises(InvalidTokenError, match="token expired"):
            SecurityPrincipal.from_header(
                f"Bearer {token}", token_secret="secret"
            )

    def test_token_within_ttl_validates_successfully(self):
        token = sign_token({"team": "ops", "role": "admin"}, "secret", ttl_seconds=3600)
        principal = SecurityPrincipal.from_header(
            f"Bearer {token}", token_secret="secret"
        )
        assert principal.team == "ops"
        assert principal.role == "admin"

    def test_token_at_exact_expiry_boundary_rejected(self):
        now = int(time.time())
        boundary_payload = {"team": "ops", "role": "viewer", "iat": now, "exp": now}
        token = jwt.encode(boundary_payload, "secret", algorithm=JWT_ALGORITHM)
        with pytest.raises(InvalidTokenError, match="token expired"):
            SecurityPrincipal.from_header(
                f"Bearer {token}", token_secret="secret"
            )

    def test_default_ttl_is_one_hour(self):
        token = sign_token({"team": "ops", "role": "admin"}, "secret")
        decoded = jwt.decode(token, "secret", algorithms=[JWT_ALGORITHM])
        assert decoded["exp"] - decoded["iat"] == 3600


class TestJWTTokenVerification:
    def test_valid_signed_token_parses_correctly(self):
        secret = "production-secret"
        token = sign_token(
            {"team": "platform", "namespace": "production", "role": "admin"},
            secret,
        )
        principal = SecurityPrincipal.from_header(
            f"Bearer {token}", token_secret=secret
        )
        assert principal.team == "platform"
        assert principal.namespace == "production"
        assert principal.role == "admin"

    def test_invalid_signature_raises_error(self):
        secret = "real-secret"
        token = sign_token({"team": "ops", "role": "admin"}, "wrong-secret")
        with pytest.raises(InvalidTokenError):
            SecurityPrincipal.from_header(
                f"Bearer {token}", token_secret=secret
            )

    def test_tampered_payload_raises_error(self):
        secret = "real-secret"
        token = sign_token({"team": "ops", "role": "viewer"}, secret)
        parts = token.split(".")
        import base64
        payload_bytes = base64.urlsafe_b64decode(parts[1] + "==")
        tampered = payload_bytes.replace(b"viewer", b"admin!")
        parts[1] = base64.urlsafe_b64encode(tampered).rstrip(b"=").decode()
        tampered_token = ".".join(parts)
        with pytest.raises(InvalidTokenError):
            SecurityPrincipal.from_header(
                f"Bearer {tampered_token}", token_secret=secret
            )

    def test_nonsense_token_raises_error(self):
        with pytest.raises(InvalidTokenError):
            SecurityPrincipal.from_header(
                "Bearer not.a.jwt", token_secret="secret"
            )

    def test_empty_header_still_returns_anonymous(self):
        principal = SecurityPrincipal.from_header("", token_secret="some-secret")
        assert principal.role == "anonymous"
        assert principal.team == "*"

    def test_none_header_still_returns_anonymous(self):
        principal = SecurityPrincipal.from_header(None, token_secret="some-secret")
        assert principal.role == "anonymous"
        assert principal.team == "*"

    def test_no_secret_rejects_token_fail_closed(self):
        with pytest.raises(InvalidTokenError, match="no secret configured"):
            SecurityPrincipal.from_header(
                "Bearer team=ops,role=admin", token_secret=""
            )

    def test_no_secret_default_rejects_token_fail_closed(self):
        with pytest.raises(InvalidTokenError, match="no secret configured"):
            SecurityPrincipal.from_header(
                "Bearer team=ops,role=admin"
            )


class TestRequireVerificationFlag:
    def test_token_with_require_and_no_secret_raises(self):
        with pytest.raises(InvalidTokenError, match="no secret configured"):
            SecurityPrincipal.from_header(
                "Bearer team=ops,role=admin",
                token_secret="",
                require_verification=True,
            )

    def test_no_token_with_require_returns_anonymous(self):
        principal = SecurityPrincipal.from_header(
            "",
            token_secret="",
            require_verification=True,
        )
        assert principal.role == "anonymous"

    def test_token_with_require_and_secret_verifies(self):
        token = sign_token({"team": "ops", "role": "viewer"}, "s3cret")
        principal = SecurityPrincipal.from_header(
            f"Bearer {token}",
            token_secret="s3cret",
            require_verification=True,
        )
        assert principal.team == "ops"
        assert principal.role == "viewer"

    def test_forged_token_with_require_and_secret_rejected(self):
        with pytest.raises(InvalidTokenError):
            SecurityPrincipal.from_header(
                "Bearer forged.token.here",
                token_secret="real-secret",
                require_verification=True,
            )


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
