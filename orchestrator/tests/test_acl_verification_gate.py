import pytest

from orchestrator.app.access_control import (
    ACLCoverageError,
    CypherPermissionFilter,
    SecurityPrincipal,
)


def _viewer_filter() -> CypherPermissionFilter:
    return CypherPermissionFilter(
        SecurityPrincipal(team="platform", namespace="production", role="viewer")
    )


def _admin_filter() -> CypherPermissionFilter:
    return CypherPermissionFilter(
        SecurityPrincipal(team="platform", namespace="production", role="admin")
    )


class TestACLVerificationGateSimpleQueries:
    def test_simple_match_passes_verification(self):
        filt = _viewer_filter()
        cypher = "MATCH (n:Service) RETURN n"
        injected, params = filt.inject_into_cypher(cypher)
        assert "n.team_owner" in injected
        assert params["acl_team"] == "platform"

    def test_admin_bypasses_verification(self):
        filt = _admin_filter()
        cypher = "MATCH (n:Service) RETURN n"
        injected, params = filt.inject_into_cypher(cypher)
        assert injected == cypher
        assert params == {}


class TestACLVerificationGateComplexQueries:
    def test_union_query_verified_all_branches(self):
        filt = _viewer_filter()
        cypher = (
            "MATCH (n:Service) RETURN n "
            "UNION "
            "MATCH (n:Service) RETURN n"
        )
        injected, params = filt.inject_into_cypher(cypher)
        acl_count = injected.count("n.team_owner")
        assert acl_count >= 2

    def test_call_subquery_verified(self):
        filt = _viewer_filter()
        cypher = (
            "MATCH (n:Service) "
            "CALL { WITH n MATCH (n)-[:CALLS]->(t) RETURN t } "
            "RETURN n, t"
        )
        injected, params = filt.inject_into_cypher(cypher)
        acl_count = injected.count("n.team_owner")
        assert acl_count >= 2


class TestACLCoverageErrorRaised:
    def test_inject_passes_when_all_scopes_covered(self):
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer",
        )
        filt = CypherPermissionFilter(principal, verify_coverage=True)
        cypher = "MATCH (n:Service) RETURN n"
        injected, params = filt.inject_into_cypher(cypher)
        assert "n.team_owner" in injected

    def test_inject_raises_acl_coverage_error_on_validation_failure(self):
        from unittest.mock import patch
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer",
        )
        filt = CypherPermissionFilter(principal, verify_coverage=True)
        cypher = "MATCH (n:Service) RETURN n"
        with patch(
            "orchestrator.app.access_control.validate_acl_coverage",
            return_value=False,
        ):
            with pytest.raises(ACLCoverageError):
                filt.inject_into_cypher(cypher)

    def test_no_error_when_verify_coverage_disabled(self):
        from unittest.mock import patch
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="viewer",
        )
        filt = CypherPermissionFilter(principal, verify_coverage=False)
        cypher = "MATCH (n:Service) RETURN n"
        with patch(
            "orchestrator.app.access_control.validate_acl_coverage",
            return_value=False,
        ):
            injected, _ = filt.inject_into_cypher(cypher)
            assert "n.team_owner" in injected

    def test_verify_coverage_flag_defaults_true(self):
        filt = CypherPermissionFilter(
            SecurityPrincipal(team="eng", namespace="staging", role="viewer"),
        )
        assert filt.verify_coverage is True

    def test_coverage_error_type_is_importable(self):
        assert issubclass(ACLCoverageError, Exception)
