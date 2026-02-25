from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

_audit_logger = logging.getLogger("graphrag.security.audit")


class AuditAction(Enum):
    AUTH_SUCCESS = "auth.success"
    AUTH_FAILURE = "auth.failure"
    QUERY_EXECUTE = "query.execute"
    QUERY_REJECT = "query.reject"
    INGEST_START = "ingest.start"
    INGEST_COMPLETE = "ingest.complete"
    INGEST_FAIL = "ingest.fail"
    RATE_LIMIT_HIT = "rate_limit.hit"
    TENANT_ACCESS = "tenant.access"
    ADMIN_ACTION = "admin.action"
    CONFIG_CHANGE = "config.change"


@dataclass(frozen=True)
class AuditEvent:
    action: AuditAction
    tenant_id: str = ""
    principal: str = ""
    resource: str = ""
    detail: str = ""
    outcome: str = "success"
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class SecurityAuditLogger:
    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._logger = logger or _audit_logger
        self._buffer: List[AuditEvent] = []

    def log(self, event: AuditEvent) -> None:
        entry = {
            "action": event.action.value,
            "tenant_id": event.tenant_id,
            "principal": event.principal,
            "resource": event.resource,
            "detail": event.detail,
            "outcome": event.outcome,
            "timestamp": event.timestamp,
            **event.metadata,
        }
        self._logger.info(json.dumps(entry, default=str))
        self._buffer.append(event)

    def log_auth(
        self,
        principal: str,
        success: bool,
        tenant_id: str = "",
        detail: str = "",
    ) -> None:
        action = AuditAction.AUTH_SUCCESS if success else AuditAction.AUTH_FAILURE
        self.log(AuditEvent(
            action=action,
            tenant_id=tenant_id,
            principal=principal,
            detail=detail,
            outcome="success" if success else "failure",
        ))

    def log_query(
        self,
        tenant_id: str,
        principal: str,
        query_hash: str,
        complexity: str = "",
        result_count: int = 0,
        duration_ms: float = 0.0,
    ) -> None:
        self.log(AuditEvent(
            action=AuditAction.QUERY_EXECUTE,
            tenant_id=tenant_id,
            principal=principal,
            resource=f"query:{query_hash}",
            metadata={
                "complexity": complexity,
                "result_count": result_count,
                "duration_ms": round(duration_ms, 2),
            },
        ))

    def log_query_reject(
        self,
        tenant_id: str,
        principal: str,
        reason: str,
    ) -> None:
        self.log(AuditEvent(
            action=AuditAction.QUERY_REJECT,
            tenant_id=tenant_id,
            principal=principal,
            detail=reason,
            outcome="rejected",
        ))

    def log_rate_limit(
        self,
        tenant_id: str,
        principal: str = "",
    ) -> None:
        self.log(AuditEvent(
            action=AuditAction.RATE_LIMIT_HIT,
            tenant_id=tenant_id,
            principal=principal,
            outcome="throttled",
        ))

    def recent_events(self, limit: int = 100) -> List[AuditEvent]:
        return list(self._buffer[-limit:])

    def clear_buffer(self) -> None:
        self._buffer.clear()
