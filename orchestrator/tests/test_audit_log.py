from __future__ import annotations

import json
import logging
from typing import List

import pytest

from orchestrator.app.audit_log import (
    AuditAction,
    AuditEvent,
    SecurityAuditLogger,
)


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record.getMessage())


def _make_logger() -> tuple[SecurityAuditLogger, _CaptureHandler]:
    handler = _CaptureHandler()
    log = logging.getLogger("test.audit")
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    audit = SecurityAuditLogger(logger=log)
    return audit, handler


class TestAuditEventStructure:

    def test_event_has_required_fields(self) -> None:
        event = AuditEvent(
            action=AuditAction.AUTH_SUCCESS,
            tenant_id="t1",
            principal="user@example.com",
        )
        assert event.action == AuditAction.AUTH_SUCCESS
        assert event.tenant_id == "t1"
        assert event.principal == "user@example.com"
        assert event.timestamp > 0

    def test_event_defaults(self) -> None:
        event = AuditEvent(action=AuditAction.QUERY_EXECUTE)
        assert event.tenant_id == ""
        assert event.outcome == "success"
        assert event.metadata == {}


class TestSecurityAuditLogger:

    def test_log_emits_json(self) -> None:
        audit, handler = _make_logger()
        audit.log(AuditEvent(
            action=AuditAction.AUTH_SUCCESS,
            tenant_id="acme",
            principal="admin",
        ))
        assert len(handler.records) == 1
        parsed = json.loads(handler.records[0])
        assert parsed["action"] == "auth.success"
        assert parsed["tenant_id"] == "acme"
        assert parsed["principal"] == "admin"

    def test_log_auth_success(self) -> None:
        audit, handler = _make_logger()
        audit.log_auth(principal="user1", success=True, tenant_id="t1")
        parsed = json.loads(handler.records[0])
        assert parsed["action"] == "auth.success"
        assert parsed["outcome"] == "success"

    def test_log_auth_failure(self) -> None:
        audit, handler = _make_logger()
        audit.log_auth(
            principal="attacker",
            success=False,
            detail="invalid token",
        )
        parsed = json.loads(handler.records[0])
        assert parsed["action"] == "auth.failure"
        assert parsed["outcome"] == "failure"
        assert "invalid token" in parsed["detail"]

    def test_log_query(self) -> None:
        audit, handler = _make_logger()
        audit.log_query(
            tenant_id="t1",
            principal="user1",
            query_hash="abc123",
            complexity="MULTI_HOP",
            result_count=42,
            duration_ms=150.5,
        )
        parsed = json.loads(handler.records[0])
        assert parsed["action"] == "query.execute"
        assert parsed["complexity"] == "MULTI_HOP"
        assert parsed["result_count"] == 42
        assert parsed["duration_ms"] == 150.5

    def test_log_query_reject(self) -> None:
        audit, handler = _make_logger()
        audit.log_query_reject(
            tenant_id="t1",
            principal="user1",
            reason="cost exceeds limit",
        )
        parsed = json.loads(handler.records[0])
        assert parsed["action"] == "query.reject"
        assert parsed["outcome"] == "rejected"

    def test_log_rate_limit(self) -> None:
        audit, handler = _make_logger()
        audit.log_rate_limit(tenant_id="t1")
        parsed = json.loads(handler.records[0])
        assert parsed["action"] == "rate_limit.hit"
        assert parsed["outcome"] == "throttled"

    def test_recent_events_returns_buffer(self) -> None:
        audit, _ = _make_logger()
        audit.log_auth(principal="u1", success=True)
        audit.log_auth(principal="u2", success=False)
        events = audit.recent_events()
        assert len(events) == 2
        assert events[0].action == AuditAction.AUTH_SUCCESS
        assert events[1].action == AuditAction.AUTH_FAILURE

    def test_clear_buffer(self) -> None:
        audit, _ = _make_logger()
        audit.log_auth(principal="u1", success=True)
        audit.clear_buffer()
        assert len(audit.recent_events()) == 0


class TestAuditActionEnum:

    def test_all_actions_have_dotted_values(self) -> None:
        for action in AuditAction:
            assert "." in action.value, (
                f"AuditAction.{action.name} value {action.value!r} "
                f"should use dotted notation"
            )

    def test_covers_required_security_events(self) -> None:
        required = {
            "auth.success", "auth.failure",
            "query.execute", "query.reject",
            "ingest.start", "ingest.complete",
            "rate_limit.hit", "tenant.access",
        }
        actual = {a.value for a in AuditAction}
        missing = required - actual
        assert not missing, f"Missing required audit actions: {missing}"
