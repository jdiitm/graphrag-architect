from __future__ import annotations

import pytest

from orchestrator.app.access_control import SecurityPrincipal
from orchestrator.app.graph_api import enforce_cypher_tenant_scope


class TestEnforceCypherTenantScope:
    def test_wildcard_tenant_raises(self) -> None:
        with pytest.raises(ValueError, match="wildcard"):
            enforce_cypher_tenant_scope(
                "MATCH (n) WHERE n.tenant_id = $tenant_id RETURN n", "*",
            )

    def test_valid_tenant_with_param_passes(self) -> None:
        enforce_cypher_tenant_scope(
            "MATCH (n) WHERE n.tenant_id = $tenant_id RETURN n", "acme",
        )

    def test_empty_tenant_raises(self) -> None:
        with pytest.raises(ValueError):
            enforce_cypher_tenant_scope(
                "MATCH (n) WHERE n.tenant_id = $tenant_id RETURN n", "",
            )

    def test_missing_param_raises(self) -> None:
        with pytest.raises(ValueError):
            enforce_cypher_tenant_scope("MATCH (n) RETURN n", "acme")


class TestSecurityPrincipalWildcardRemoval:
    def test_empty_header_uses_anonymous_not_wildcard(self) -> None:
        principal = SecurityPrincipal.from_header("")
        assert principal.team != "*"
        assert principal.team == "__anonymous__"
        assert principal.role == "anonymous"

    def test_none_header_uses_anonymous_not_wildcard(self) -> None:
        principal = SecurityPrincipal.from_header(None)
        assert principal.team != "*"
        assert principal.team == "__anonymous__"

    def test_whitespace_header_uses_anonymous_not_wildcard(self) -> None:
        principal = SecurityPrincipal.from_header("   ")
        assert principal.team != "*"
        assert principal.team == "__anonymous__"

    def test_valid_token_missing_team_uses_anonymous(self) -> None:
        from unittest.mock import patch

        claims = {"namespace": "prod", "role": "viewer"}
        with patch(
            "orchestrator.app.access_control._verify_token",
            return_value=claims,
        ):
            principal = SecurityPrincipal.from_header(
                "Bearer tok", token_secret="secret",
            )
        assert principal.team != "*"
        assert principal.team == "__anonymous__"

    def test_valid_token_with_team_preserves_team(self) -> None:
        from unittest.mock import patch

        claims = {"team": "acme", "namespace": "prod", "role": "admin"}
        with patch(
            "orchestrator.app.access_control._verify_token",
            return_value=claims,
        ):
            principal = SecurityPrincipal.from_header(
                "Bearer tok", token_secret="secret",
            )
        assert principal.team == "acme"
