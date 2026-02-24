from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.request_context import (
    RequestContext,
    RequestContextFactory,
)


class TestRequestContext:
    def test_creates_with_required_fields(self) -> None:
        ctx = RequestContext(
            neo4j_session=MagicMock(),
            principal=None,
            trace_context=None,
        )
        assert ctx.neo4j_session is not None
        assert ctx.principal is None

    def test_principal_attached(self) -> None:
        principal = MagicMock()
        principal.team = "platform"
        ctx = RequestContext(
            neo4j_session=MagicMock(),
            principal=principal,
            trace_context=None,
        )
        assert ctx.principal.team == "platform"

    def test_is_isolated_per_instance(self) -> None:
        session_a = MagicMock()
        session_b = MagicMock()
        ctx_a = RequestContext(
            neo4j_session=session_a, principal=None, trace_context=None,
        )
        ctx_b = RequestContext(
            neo4j_session=session_b, principal=None, trace_context=None,
        )
        assert ctx_a.neo4j_session is not ctx_b.neo4j_session


class TestRequestContextFactory:
    def test_create_returns_context(self) -> None:
        driver = MagicMock()
        driver.session.return_value = MagicMock()
        factory = RequestContextFactory(driver=driver)
        ctx = factory.create(principal=None)
        assert isinstance(ctx, RequestContext)

    def test_create_with_principal(self) -> None:
        driver = MagicMock()
        driver.session.return_value = MagicMock()
        principal = MagicMock()
        principal.team = "backend"
        factory = RequestContextFactory(driver=driver)
        ctx = factory.create(principal=principal)
        assert ctx.principal.team == "backend"

    def test_factory_uses_driver_session(self) -> None:
        driver = MagicMock()
        mock_session = MagicMock()
        driver.session.return_value = mock_session
        factory = RequestContextFactory(driver=driver)
        ctx = factory.create(principal=None)
        assert ctx.neo4j_session is mock_session
        driver.session.assert_called_once()

    def test_concurrent_creates_give_different_sessions(self) -> None:
        driver = MagicMock()
        sessions = [MagicMock(), MagicMock()]
        driver.session.side_effect = sessions
        factory = RequestContextFactory(driver=driver)
        ctx_a = factory.create(principal=None)
        ctx_b = factory.create(principal=None)
        assert ctx_a.neo4j_session is not ctx_b.neo4j_session
