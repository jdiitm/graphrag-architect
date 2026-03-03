"""Tests for audit log secret redaction."""
import json
import logging
from unittest.mock import MagicMock

from orchestrator.app.audit_log import AuditAction, AuditEvent, SecurityAuditLogger
from orchestrator.app.tenant_isolation import StructuredTenantAuditLogger


class TestStructuredAuditLogRedaction:
    def test_redacts_aws_key_from_raw_user_query(self) -> None:
        mock_logger = MagicMock(spec=logging.Logger)
        audit = StructuredTenantAuditLogger()
        audit._logger = mock_logger
        audit.log_query(
            tenant_id="t1",
            query_hash="abc",
            result_count=1,
            raw_user_query="find key AKIAIOSFODNN7EXAMPLE",
        )
        logged = mock_logger.info.call_args[0][0]
        parsed = json.loads(logged)
        assert "AKIAIOSFODNN7EXAMPLE" not in parsed["raw_user_query"]
        assert "[REDACTED-" in parsed["raw_user_query"]

    def test_redacts_github_token_from_cypher_query(self) -> None:
        mock_logger = MagicMock(spec=logging.Logger)
        audit = StructuredTenantAuditLogger()
        audit._logger = mock_logger
        audit.log_query(
            tenant_id="t1",
            query_hash="abc",
            result_count=1,
            cypher_query=(
                "MATCH (n {key: 'ghp_abcdefghijklmnopqrstuvwxyz1234567890'}) RETURN n"
            ),
        )
        logged = mock_logger.info.call_args[0][0]
        parsed = json.loads(logged)
        assert "ghp_" not in parsed["cypher_query"]
        assert "[REDACTED-" in parsed["cypher_query"]

    def test_passes_through_safe_content(self) -> None:
        mock_logger = MagicMock(spec=logging.Logger)
        audit = StructuredTenantAuditLogger()
        audit._logger = mock_logger
        audit.log_query(
            tenant_id="t1",
            query_hash="abc",
            result_count=5,
            raw_user_query="what services call auth-service?",
            cypher_query="MATCH (n:Service) RETURN n",
        )
        logged = mock_logger.info.call_args[0][0]
        parsed = json.loads(logged)
        assert parsed["raw_user_query"] == "what services call auth-service?"
        assert parsed["cypher_query"] == "MATCH (n:Service) RETURN n"


class TestSecurityAuditLoggerRedaction:
    def test_redacts_secrets_from_detail(self) -> None:
        mock_logger = MagicMock(spec=logging.Logger)
        audit = SecurityAuditLogger(logger=mock_logger)
        event = AuditEvent(
            action=AuditAction.QUERY_EXECUTE,
            tenant_id="t1",
            detail="MATCH (n {key: 'AKIAIOSFODNN7EXAMPLE'}) RETURN n",
        )
        audit.log(event)
        logged = mock_logger.info.call_args[0][0]
        parsed = json.loads(logged)
        assert "AKIAIOSFODNN7EXAMPLE" not in parsed["detail"]
        assert "[REDACTED-" in parsed["detail"]

    def test_redacts_secrets_from_raw_user_query_metadata(self) -> None:
        mock_logger = MagicMock(spec=logging.Logger)
        audit = SecurityAuditLogger(logger=mock_logger)
        audit.log_query(
            tenant_id="t1",
            principal="admin",
            query_hash="h1",
            raw_user_query="token ghp_abcdefghijklmnopqrstuvwxyz1234567890",
            cypher_query="MATCH (n {k: 'AKIAIOSFODNN7EXAMPLE'}) RETURN n",
        )
        logged = mock_logger.info.call_args[0][0]
        parsed = json.loads(logged)
        assert "ghp_" not in parsed["raw_user_query"]
        assert "AKIAIOSFODNN7EXAMPLE" not in parsed["cypher_query"]
