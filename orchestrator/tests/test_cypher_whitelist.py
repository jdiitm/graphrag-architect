from __future__ import annotations

import hashlib
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.query_templates import TemplateCatalog


class TestTemplateHashRegistry:
    def test_registry_computes_sha256_at_init(self) -> None:
        from orchestrator.app.cypher_sandbox import TemplateHashRegistry

        catalog = TemplateCatalog()
        registry = TemplateHashRegistry(catalog)
        templates = catalog.all_templates()
        assert len(registry.registered_hashes) == len(templates)

    def test_known_template_passes_verification(self) -> None:
        from orchestrator.app.cypher_sandbox import TemplateHashRegistry

        catalog = TemplateCatalog()
        registry = TemplateHashRegistry(catalog)
        template = catalog.get("blast_radius")
        assert template is not None
        assert registry.is_allowed(template.cypher) is True

    def test_unknown_cypher_rejected(self) -> None:
        from orchestrator.app.cypher_sandbox import TemplateHashRegistry

        catalog = TemplateCatalog()
        registry = TemplateHashRegistry(catalog)
        assert registry.is_allowed("MATCH (n) DETACH DELETE n") is False

    def test_hash_is_deterministic(self) -> None:
        from orchestrator.app.cypher_sandbox import TemplateHashRegistry

        catalog = TemplateCatalog()
        r1 = TemplateHashRegistry(catalog)
        r2 = TemplateHashRegistry(catalog)
        assert r1.registered_hashes == r2.registered_hashes

    def test_whitespace_normalized_before_hashing(self) -> None:
        from orchestrator.app.cypher_sandbox import TemplateHashRegistry

        catalog = TemplateCatalog()
        registry = TemplateHashRegistry(catalog)
        template = catalog.get("blast_radius")
        assert template is not None
        padded = "  " + template.cypher + "  "
        assert registry.is_allowed(padded) is True


class TestWhitelistSandboxedExecutor:
    @pytest.mark.asyncio
    async def test_registered_template_executes(self) -> None:
        from orchestrator.app.cypher_sandbox import (
            SandboxedQueryExecutor,
            TemplateHashRegistry,
        )

        catalog = TemplateCatalog()
        registry = TemplateHashRegistry(catalog)
        executor = SandboxedQueryExecutor(registry=registry)

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(return_value=[{"count": 1}])

        template = catalog.get("blast_radius")
        assert template is not None
        result = await executor.execute_read(
            mock_session, template.cypher, {"name": "svc-a", "tenant_id": "t1"},
        )
        assert result == [{"count": 1}]

    @pytest.mark.asyncio
    async def test_unregistered_cypher_raises(self) -> None:
        from orchestrator.app.cypher_sandbox import (
            CypherWhitelistError,
            SandboxedQueryExecutor,
            TemplateHashRegistry,
        )

        catalog = TemplateCatalog()
        registry = TemplateHashRegistry(catalog)
        executor = SandboxedQueryExecutor(registry=registry)

        mock_session = AsyncMock()
        with pytest.raises(CypherWhitelistError):
            await executor.execute_read(
                mock_session, "MATCH (n) RETURN n", {},
            )

    @pytest.mark.asyncio
    async def test_executor_without_registry_allows_all(self) -> None:
        from orchestrator.app.cypher_sandbox import SandboxedQueryExecutor

        executor = SandboxedQueryExecutor()
        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(return_value=[{"ok": True}])
        result = await executor.execute_read(
            mock_session, "MATCH (n) RETURN n", {},
        )
        assert result == [{"ok": True}]


class TestCypherWhitelistError:
    def test_is_exception(self) -> None:
        from orchestrator.app.cypher_sandbox import CypherWhitelistError

        err = CypherWhitelistError("blocked")
        assert isinstance(err, Exception)
        assert "blocked" in str(err)
