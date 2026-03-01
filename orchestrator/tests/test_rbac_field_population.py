from __future__ import annotations

import textwrap
from typing import List
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.access_control import CypherPermissionFilter, SecurityPrincipal
from orchestrator.app.extraction_models import (
    CallsEdge,
    K8sDeploymentNode,
    KafkaTopicNode,
    ServiceExtractionResult,
    ServiceNode,
)
from orchestrator.app.llm_extraction import (
    SYSTEM_PROMPT,
    ServiceExtractor,
    _apply_acl_defaults,
    _infer_team_owner_from_paths,
)
from orchestrator.app.manifest_parser import (
    parse_all_manifests,
    parse_k8s_manifests,
    parse_kafka_topics,
)


class TestSystemPromptAclGuidance:

    def test_system_prompt_mentions_team_owner(self) -> None:
        assert "team_owner" in SYSTEM_PROMPT

    def test_system_prompt_mentions_directory_path_hint(self) -> None:
        assert "directory" in SYSTEM_PROMPT.lower() or "path" in SYSTEM_PROMPT.lower()


class TestInferTeamOwnerFromPaths:

    def test_services_directory_convention(self) -> None:
        paths = ["services/auth-team/cmd/main.go"]
        assert _infer_team_owner_from_paths(paths) == "auth-team"

    def test_teams_directory_convention(self) -> None:
        paths = ["teams/platform/service.py"]
        assert _infer_team_owner_from_paths(paths) == "platform"

    def test_apps_directory_convention(self) -> None:
        paths = ["apps/billing/main.go"]
        assert _infer_team_owner_from_paths(paths) == "billing"

    def test_no_hint_directory_returns_none(self) -> None:
        paths = ["main.go", "utils/helper.py"]
        assert _infer_team_owner_from_paths(paths) is None

    def test_hint_directory_without_team_segment_returns_none(self) -> None:
        paths = ["services/main.go"]
        assert _infer_team_owner_from_paths(paths) is None

    def test_empty_paths_returns_none(self) -> None:
        assert _infer_team_owner_from_paths([]) is None

    def test_first_matching_path_wins(self) -> None:
        paths = [
            "lib/utils.go",
            "services/data-team/processor.go",
            "services/other-team/handler.go",
        ]
        assert _infer_team_owner_from_paths(paths) == "data-team"

    def test_nested_path_extracts_first_team_segment(self) -> None:
        paths = ["internal/auth/handlers/login.go"]
        assert _infer_team_owner_from_paths(paths) == "auth"


class TestApplyAclDefaults:

    def test_sets_read_roles_on_service_without_roles(self) -> None:
        result = ServiceExtractionResult(
            services=[
                ServiceNode(
                    id="auth-svc", name="auth-svc", language="go",
                    framework="gin", opentelemetry_enabled=False,
                    tenant_id="test-tenant",
                ),
            ],
            calls=[],
        )
        patched = _apply_acl_defaults(result, ["cmd/main.go"])
        assert patched.services[0].read_roles == ["reader"]

    def test_preserves_existing_read_roles(self) -> None:
        result = ServiceExtractionResult(
            services=[
                ServiceNode(
                    id="auth-svc", name="auth-svc", language="go",
                    framework="gin", opentelemetry_enabled=False,
                    tenant_id="test-tenant",
                    read_roles=["admin", "writer"],
                ),
            ],
            calls=[],
        )
        patched = _apply_acl_defaults(result, ["cmd/main.go"])
        assert patched.services[0].read_roles == ["admin", "writer"]

    def test_infers_team_owner_from_paths(self) -> None:
        result = ServiceExtractionResult(
            services=[
                ServiceNode(
                    id="auth-svc", name="auth-svc", language="go",
                    framework="gin", opentelemetry_enabled=False,
                    tenant_id="test-tenant",
                ),
            ],
            calls=[],
        )
        patched = _apply_acl_defaults(
            result, ["services/platform-team/cmd/main.go"],
        )
        assert patched.services[0].team_owner == "platform-team"

    def test_preserves_explicit_team_owner(self) -> None:
        result = ServiceExtractionResult(
            services=[
                ServiceNode(
                    id="auth-svc", name="auth-svc", language="go",
                    framework="gin", opentelemetry_enabled=False,
                    tenant_id="test-tenant",
                    team_owner="explicit-team",
                ),
            ],
            calls=[],
        )
        patched = _apply_acl_defaults(
            result, ["services/inferred-team/cmd/main.go"],
        )
        assert patched.services[0].team_owner == "explicit-team"

    def test_preserves_calls(self) -> None:
        result = ServiceExtractionResult(
            services=[],
            calls=[
                CallsEdge(
                    source_service_id="a", target_service_id="b",
                    protocol="http", tenant_id="test-tenant",
                ),
            ],
        )
        patched = _apply_acl_defaults(result, ["cmd/main.go"])
        assert len(patched.calls) == 1
        assert patched.calls[0].source_service_id == "a"

    def test_handles_multiple_services(self) -> None:
        result = ServiceExtractionResult(
            services=[
                ServiceNode(
                    id="svc-a", name="svc-a", language="go",
                    framework="gin", opentelemetry_enabled=False,
                    tenant_id="test-tenant",
                ),
                ServiceNode(
                    id="svc-b", name="svc-b", language="python",
                    framework="fastapi", opentelemetry_enabled=True,
                    tenant_id="test-tenant",
                    team_owner="already-set",
                ),
            ],
            calls=[],
        )
        patched = _apply_acl_defaults(
            result, ["services/my-team/main.go"],
        )
        assert patched.services[0].read_roles == ["reader"]
        assert patched.services[0].team_owner == "my-team"
        assert patched.services[1].read_roles == ["reader"]
        assert patched.services[1].team_owner == "already-set"


class TestExtractAllAppliesAclDefaults:

    @pytest.mark.asyncio
    async def test_extract_all_populates_read_roles(self) -> None:
        mock_result = ServiceExtractionResult(
            services=[
                ServiceNode(
                    id="test-svc", name="test-svc", language="go",
                    framework="gin", opentelemetry_enabled=False,
                    tenant_id="test-tenant",
                ),
            ],
            calls=[],
        )

        extractor = ServiceExtractor.__new__(ServiceExtractor)
        extractor.config = _make_test_config()
        extractor.chain = AsyncMock(return_value=mock_result)

        result = await extractor.extract_all(
            [{"path": "services/platform/cmd/main.go", "content": "package main"}],
        )
        assert len(result.services) == 1
        assert result.services[0].read_roles == ["reader"]
        assert result.services[0].team_owner == "platform"

    @pytest.mark.asyncio
    async def test_extract_all_no_files_returns_empty(self) -> None:
        extractor = ServiceExtractor.__new__(ServiceExtractor)
        extractor.config = _make_test_config()
        extractor.chain = AsyncMock()

        result = await extractor.extract_all([])
        assert result.services == []
        assert result.calls == []


class TestAclFilterMatchesPopulatedNodes:

    def test_reader_role_matches_node_with_default_read_roles(self) -> None:
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="reader",
        )
        pf = CypherPermissionFilter(principal)
        clause, params = pf.node_filter("n")
        assert params["acl_role"] == "reader"
        assert "$acl_role IN n.read_roles" in clause

    def test_namespace_scoped_user_matches_populated_namespace_acl(self) -> None:
        principal = SecurityPrincipal(
            team="platform", namespace="production", role="reader",
        )
        pf = CypherPermissionFilter(principal)
        clause, params = pf.node_filter("n")
        assert params["acl_namespace"] == "production"
        assert "$acl_namespace IN n.namespace_acl" in clause

    def test_manifest_parsed_node_has_all_acl_fields_for_matching(self) -> None:
        content = textwrap.dedent("""\
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: api-gateway
              namespace: production
              labels:
                graphrag.io/team-owner: platform
              annotations:
                graphrag.io/namespace-acl: "production,staging"
            spec:
              replicas: 3
        """)
        nodes = parse_k8s_manifests(content)
        assert len(nodes) == 1
        node = nodes[0]
        assert node.team_owner == "platform"
        assert "production" in node.namespace_acl
        assert node.read_roles == ["reader"]


def _make_test_config():
    from orchestrator.app.config import ExtractionConfig
    return ExtractionConfig(
        google_api_key="fake-key",
        model_name="gemini-2.0-flash",
        token_budget_per_batch=100000,
        max_concurrency=1,
        max_retries=1,
        retry_min_wait=0,
        retry_max_wait=0,
    )
