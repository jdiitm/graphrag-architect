from __future__ import annotations

import os
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.app.tenant_isolation import (
    IsolationMode,
    TenantEnforcingDriver,
    TenantIsolationViolation,
)


def _make_mock_driver() -> MagicMock:
    mock_session = AsyncMock()
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    mock_driver = MagicMock()
    mock_driver.session.return_value = ctx
    return mock_driver


class TestAdminTenantEnforcement:

    @pytest.mark.asyncio
    async def test_tenant_scoped_session_rejects_missing_tenant_param(self) -> None:
        from orchestrator.app.tenant_admin_api import _tenant_scoped_session

        mock_driver = _make_mock_driver()

        async with _tenant_scoped_session(
            mock_driver,
            tenant_id="acme",
            database="neo4j",
            isolation_mode=IsolationMode.LOGICAL,
        ) as session:
            with pytest.raises(TenantIsolationViolation):
                await session.execute_read(
                    AsyncMock(),
                    "MATCH (n) RETURN n",
                    {},
                )

    @pytest.mark.asyncio
    async def test_tenant_scoped_session_allows_valid_query(self) -> None:
        from orchestrator.app.tenant_admin_api import _tenant_scoped_session

        mock_driver = _make_mock_driver()

        async with _tenant_scoped_session(
            mock_driver,
            tenant_id="acme",
            database="neo4j",
            isolation_mode=IsolationMode.LOGICAL,
        ) as session:
            assert session is not None

    @pytest.mark.asyncio
    async def test_physical_isolation_skips_param_check(self) -> None:
        from orchestrator.app.tenant_admin_api import _tenant_scoped_session

        mock_driver = _make_mock_driver()

        async with _tenant_scoped_session(
            mock_driver,
            tenant_id="acme",
            database="neo4j",
            isolation_mode=IsolationMode.PHYSICAL,
        ) as session:
            await session.execute_read(
                AsyncMock(),
                "MATCH (n) RETURN n",
                {},
            )


class TestCallIsolationWiring:

    def test_query_engine_validates_call_subquery_acl(self) -> None:
        from orchestrator.app.query_engine import validate_cypher_security

        cypher_with_call = "CALL { MATCH (n:Service) RETURN n } RETURN n"
        with pytest.raises(Exception):
            validate_cypher_security(cypher_with_call, {"tenant_id": "acme"})

    def test_query_engine_passes_safe_call_subquery(self) -> None:
        from orchestrator.app.query_engine import validate_cypher_security

        safe_cypher = (
            "CALL { MATCH (n:Service) "
            "WHERE n.tenant_id = $tenant_id "
            "AND n.team_owner = $acl_team "
            "AND n.namespace_acl IN $acl_labels "
            "AND ($is_admin OR ANY(lbl IN labels(n) WHERE lbl IN $acl_labels)) "
            "RETURN n } RETURN n"
        )
        validate_cypher_security(safe_cypher, {"tenant_id": "acme"})


class TestTenantSecurityWiring:

    def test_template_cypher_validated_for_tenant(self) -> None:
        from orchestrator.app.query_engine import validate_cypher_security

        cypher_no_tenant = "MATCH (n) RETURN n"
        with pytest.raises(Exception):
            validate_cypher_security(cypher_no_tenant, {})

    def test_template_cypher_with_tenant_passes(self) -> None:
        from orchestrator.app.query_engine import validate_cypher_security

        cypher_with_tenant = (
            "MATCH (n) WHERE n.tenant_id = $tenant_id "
            "AND ($is_admin OR ANY(lbl IN labels(n) WHERE lbl IN $acl_labels)) "
            "RETURN n"
        )
        validate_cypher_security(
            cypher_with_tenant,
            {"tenant_id": "acme"},
        )


class TestRequestSigningWiring:

    def test_unsigned_ingest_in_production_returns_403(self) -> None:
        from orchestrator.app.main import _verify_request_signing

        with patch.dict(os.environ, {
            "DEPLOYMENT_MODE": "production",
            "REQUEST_SIGNING_SECRET": "test-secret-key-for-signing",
        }):
            from fastapi import HTTPException as FastAPIHTTPException
            with pytest.raises(FastAPIHTTPException) as exc_info:
                _verify_request_signing(
                    "POST", "/ingest", b"", None, None, None,
                )
            assert exc_info.value.status_code == 403

    def test_signed_ingest_in_dev_mode_skips_check(self) -> None:
        from orchestrator.app.main import _verify_request_signing

        with patch.dict(os.environ, {"DEPLOYMENT_MODE": "dev"}, clear=False):
            _verify_request_signing(
                "POST", "/ingest", b"", None, None, None,
            )


class TestServiceProfile:

    def test_query_profile_excludes_ingest(self) -> None:
        with patch.dict(os.environ, {"SERVICE_PROFILE": "query"}):
            from orchestrator.app.main import create_profiled_app
            profiled_app = create_profiled_app()
            from starlette.testclient import TestClient
            client = TestClient(profiled_app, raise_server_exceptions=False)
            response = client.post("/ingest", json={
                "documents": [{
                    "file_path": "main.go",
                    "content": "cGFja2FnZSBtYWlu",
                    "source_type": "source_code",
                }],
            })
            assert response.status_code == 404

    def test_ingest_profile_excludes_query(self) -> None:
        with patch.dict(os.environ, {"SERVICE_PROFILE": "ingest"}):
            from orchestrator.app.main import create_profiled_app
            profiled_app = create_profiled_app()
            from starlette.testclient import TestClient
            client = TestClient(profiled_app, raise_server_exceptions=False)
            response = client.post("/query", json={
                "query": "What services exist?",
            })
            assert response.status_code == 404

    def test_full_profile_serves_both(self) -> None:
        with patch.dict(os.environ, {"SERVICE_PROFILE": "full"}):
            from orchestrator.app.main import create_profiled_app
            profiled_app = create_profiled_app()
            from starlette.testclient import TestClient
            client = TestClient(profiled_app, raise_server_exceptions=False)
            ingest_resp = client.post("/ingest", json={
                "documents": [{
                    "file_path": "main.go",
                    "content": "cGFja2FnZSBtYWlu",
                    "source_type": "source_code",
                }],
            })
            query_resp = client.post("/query", json={
                "query": "What services exist?",
            })
            assert ingest_resp.status_code != 404
            assert query_resp.status_code != 404


class TestHMACDelimiterSharedSecret:

    def test_two_instances_same_key_cross_validate(self) -> None:
        from orchestrator.app.prompt_sanitizer import HMACDelimiter

        shared_key = b"deterministic-test-key-1234567890"
        instance_a = HMACDelimiter(key=shared_key)
        instance_b = HMACDelimiter(key=shared_key)

        delimiter = instance_a.generate()
        assert instance_b.validate(delimiter)

    def test_different_keys_do_not_cross_validate(self) -> None:
        from orchestrator.app.prompt_sanitizer import HMACDelimiter

        instance_a = HMACDelimiter(key=b"key-aaa-0000000000000000000000")
        instance_b = HMACDelimiter(key=b"key-bbb-0000000000000000000000")

        delimiter = instance_a.generate()
        assert not instance_b.validate(delimiter)

    def test_production_without_env_var_raises(self) -> None:
        from orchestrator.app.prompt_sanitizer import HMACDelimiter

        with patch.dict(os.environ, {"DEPLOYMENT_MODE": "production"}, clear=False):
            env = os.environ.copy()
            env.pop("HMAC_DELIMITER_SECRET", None)
            with patch.dict(os.environ, env, clear=True):
                with pytest.raises(Exception):
                    HMACDelimiter.from_env()

    def test_dev_mode_falls_back_to_random_key(self) -> None:
        from orchestrator.app.prompt_sanitizer import HMACDelimiter

        with patch.dict(os.environ, {"DEPLOYMENT_MODE": "dev"}, clear=False):
            env = os.environ.copy()
            env.pop("HMAC_DELIMITER_SECRET", None)
            with patch.dict(os.environ, env, clear=True):
                env_copy = os.environ.copy()
                env_copy["DEPLOYMENT_MODE"] = "dev"
                with patch.dict(os.environ, env_copy, clear=True):
                    instance = HMACDelimiter.from_env()
                    delimiter = instance.generate()
                    assert instance.validate(delimiter)


class TestDegradationResponse:

    @pytest.mark.asyncio
    async def test_empty_candidates_with_degraded_flag_returns_structured(self) -> None:
        from orchestrator.app.query_engine import synthesize_answer

        state: Dict[str, Any] = {
            "query": "What services call auth?",
            "candidates": [],
            "cypher_results": [],
            "retrieval_degraded": True,
            "tenant_id": "test",
            "complexity": "entity_lookup",
            "retrieval_path": "vector",
            "authorization": "",
            "max_results": 10,
            "answer": "",
            "sources": [],
            "evaluation_score": None,
            "retrieval_quality": "skipped",
            "query_id": "",
            "fusion_hint": None,
        }

        result = await synthesize_answer(state)
        assert result["answer"] != ""
        assert "degraded" in result["answer"].lower() or "unavailable" in result["answer"].lower()
        assert result.get("retrieval_degraded") is True

    @pytest.mark.asyncio
    async def test_non_degraded_path_invokes_llm(self) -> None:
        from orchestrator.app.query_engine import synthesize_answer

        state: Dict[str, Any] = {
            "query": "What services call auth?",
            "candidates": [{"name": "auth-service", "score": 0.9}],
            "cypher_results": [],
            "retrieval_degraded": False,
            "tenant_id": "test",
            "complexity": "entity_lookup",
            "retrieval_path": "vector",
            "authorization": "",
            "max_results": 10,
            "answer": "",
            "sources": [],
            "evaluation_score": None,
            "retrieval_quality": "skipped",
            "query_id": "",
            "fusion_hint": None,
        }

        with patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="auth-service handles authentication",
        ):
            result = await synthesize_answer(state)
            assert "auth" in result["answer"].lower()


class TestCDATAEncoding:

    def test_normal_content_unchanged_semantically(self) -> None:
        from orchestrator.app.context_manager import format_context_for_prompt

        context = [{"name": "auth-service", "type": "Service"}]
        block = format_context_for_prompt(context)
        assert "auth-service" in block.content
        assert "<![CDATA[" in block.content

    def test_closing_tag_stays_inside_cdata(self) -> None:
        from orchestrator.app.context_manager import format_context_for_prompt

        delimiter_tag = "</GRAPHCTX_abc123_deadbeef>"
        context = [{"payload": f"some code with {delimiter_tag} inside"}]
        block = format_context_for_prompt(context)
        cdata_start = block.content.find("<![CDATA[")
        cdata_end = block.content.rfind("]]>")
        assert cdata_start != -1
        assert cdata_end != -1
        assert cdata_end > cdata_start

    def test_cdata_end_marker_escaped(self) -> None:
        from orchestrator.app.context_manager import _cdata_escape

        escaped = _cdata_escape("data with ]]> inside")
        assert "]]]]><![CDATA[>" in escaped

        round_tripped = escaped.replace("]]]]><![CDATA[>", "]]>")
        assert round_tripped == "data with ]]> inside"

    def test_cdata_escape_no_false_close(self) -> None:
        from orchestrator.app.context_manager import _cdata_escape

        escaped = _cdata_escape("safe text")
        assert escaped == "safe text"
        assert "]]]]>" not in escaped

    def test_empty_context_returns_empty(self) -> None:
        from orchestrator.app.context_manager import format_context_for_prompt

        block = format_context_for_prompt([])
        assert block.content == ""


class TestCypherSecurityExecutionWiring:

    @pytest.mark.asyncio
    async def test_sandboxed_read_calls_validate_cypher_security(self) -> None:
        from orchestrator.app.query_engine import _execute_sandboxed_read

        mock_driver = AsyncMock()

        with patch(
            "orchestrator.app.query_engine.validate_cypher_security",
            side_effect=ValueError("security validation invoked"),
        ):
            with pytest.raises(ValueError, match="security validation invoked"):
                await _execute_sandboxed_read(
                    mock_driver,
                    "MATCH (n) RETURN n",
                    {"tenant_id": "acme"},
                )


class TestRequestSigningFailClosed:

    def test_production_mode_no_secret_raises_503(self) -> None:
        from orchestrator.app.main import _verify_request_signing

        with patch.dict(os.environ, {
            "DEPLOYMENT_MODE": "production",
            "REQUEST_SIGNING_SECRET": "",
        }):
            with pytest.raises(HTTPException) as exc_info:
                _verify_request_signing(
                    "POST", "/ingest", b"payload",
                    None, None, None,
                )
            assert exc_info.value.status_code == 503


class TestRequestBodyInSignature:

    @pytest.mark.asyncio
    async def test_ingest_passes_real_body_to_signing(self) -> None:
        from orchestrator.app.main import ingest as ingest_fn

        mock_pydantic_req = MagicMock()
        mock_pydantic_req.documents = []

        mock_raw_request = AsyncMock()
        mock_raw_request.body = AsyncMock(return_value=b'{"documents": []}')

        captured_bodies: List[bytes] = []

        def capturing_verify(
            method: str, path: str, body: bytes,
            ts: Any, nonce: Any, sig: Any,
        ) -> None:
            captured_bodies.append(body)

        with patch("orchestrator.app.main._verify_request_signing", side_effect=capturing_verify), \
             patch("orchestrator.app.main._verify_ingest_auth"), \
             patch("orchestrator.app.main._resolve_tenant_context", return_value=MagicMock(tenant_id="default")), \
             patch("orchestrator.app.main._decode_documents_async", new_callable=AsyncMock, return_value=[]):
            await ingest_fn(
                request=mock_pydantic_req,
                raw_request=mock_raw_request,
                authorization=None,
                x_signature=None,
                x_timestamp=None,
                x_nonce=None,
                sync=False,
            )

        assert len(captured_bodies) == 1
        assert captured_bodies[0] == b'{"documents": []}'
