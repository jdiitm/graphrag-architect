from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class RequestContext:
    neo4j_session: Any
    principal: Optional[Any]
    trace_context: Optional[Any]


class RequestContextFactory:
    def __init__(self, driver: Any) -> None:
        self._driver = driver

    def create(
        self,
        principal: Optional[Any] = None,
        trace_context: Optional[Any] = None,
    ) -> RequestContext:
        session = self._driver.session()
        return RequestContext(
            neo4j_session=session,
            principal=principal,
            trace_context=trace_context,
        )
