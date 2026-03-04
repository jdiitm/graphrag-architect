from __future__ import annotations

from unittest.mock import patch


def test_build_traversal_acl_params_propagates_tenant_id() -> None:
    from orchestrator.app import query_engine

    state = {
        "authorization": "",
        "tenant_id": "tenant-red",
    }
    principal = query_engine.SecurityPrincipal(
        team="team-a", namespace="prod", role="viewer",
    )
    with patch.object(
        query_engine.SecurityPrincipal,
        "from_header",
        return_value=principal,
    ):
        params = query_engine._build_traversal_acl_params(state)
    assert params["tenant_id"] == "tenant-red"
