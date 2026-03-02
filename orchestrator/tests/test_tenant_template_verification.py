from __future__ import annotations

import logging
import os
from typing import FrozenSet

import pytest

from orchestrator.app.query_engine import (
    FulltextTenantRequired,
    build_fulltext_fallback_cypher,
)
from orchestrator.app.query_templates import (
    QueryTemplate,
    TemplateCatalog,
)
from orchestrator.app.tenant_security import (
    TenantScopeVerifier,
    TenantTemplateViolationError,
)


class TestTenantScopeVerifierPassesCleanCatalog:

    def test_all_registered_templates_pass_verification(self) -> None:
        catalog = TemplateCatalog()
        verifier = TenantScopeVerifier(catalog=catalog)
        verifier.verify_all_templates()

    def test_find_unscoped_returns_empty_for_clean_catalog(self) -> None:
        catalog = TemplateCatalog()
        verifier = TenantScopeVerifier(catalog=catalog)
        assert verifier.find_unscoped_templates() == []


class TestTenantScopeVerifierDetectsUnscopedTemplate:

    def _catalog_with_unscoped_template(self) -> TemplateCatalog:
        catalog = TemplateCatalog()
        catalog._templates["unscoped_dangerous"] = QueryTemplate(
            name="unscoped_dangerous",
            cypher="MATCH (n:Service) RETURN n.name AS name LIMIT $limit",
            parameters=("limit",),
            description="Deliberately missing tenant_id for testing",
        )
        return catalog

    def test_raises_on_unscoped_template(self) -> None:
        catalog = self._catalog_with_unscoped_template()
        verifier = TenantScopeVerifier(catalog=catalog)
        with pytest.raises(TenantTemplateViolationError, match="unscoped_dangerous"):
            verifier.verify_all_templates()

    def test_find_unscoped_returns_violating_names(self) -> None:
        catalog = self._catalog_with_unscoped_template()
        verifier = TenantScopeVerifier(catalog=catalog)
        violations = verifier.find_unscoped_templates()
        assert "unscoped_dangerous" in violations

    def test_violation_error_contains_all_offending_names(self) -> None:
        catalog = TemplateCatalog()
        catalog._templates["bad_one"] = QueryTemplate(
            name="bad_one",
            cypher="MATCH (n:Service) RETURN n",
            parameters=(),
        )
        catalog._templates["bad_two"] = QueryTemplate(
            name="bad_two",
            cypher="MATCH (n:KafkaTopic) RETURN n",
            parameters=(),
        )
        verifier = TenantScopeVerifier(catalog=catalog)
        with pytest.raises(TenantTemplateViolationError) as exc_info:
            verifier.verify_all_templates()
        error_msg = str(exc_info.value)
        assert "bad_one" in error_msg
        assert "bad_two" in error_msg


class TestTenantScopeVerifierExemptTemplates:

    def test_exempt_template_bypasses_verification(self) -> None:
        catalog = TemplateCatalog()
        catalog._templates["health_check"] = QueryTemplate(
            name="health_check",
            cypher="CALL dbms.components() YIELD edition RETURN edition",
            parameters=(),
            description="Health check query without tenant scope",
        )
        exempt: FrozenSet[str] = frozenset({"health_check"})
        verifier = TenantScopeVerifier(catalog=catalog, exempt_templates=exempt)
        verifier.verify_all_templates()

    def test_exempt_whitelist_is_auditable(self) -> None:
        catalog = TemplateCatalog()
        exempt: FrozenSet[str] = frozenset({"schema_version", "health_check"})
        verifier = TenantScopeVerifier(catalog=catalog, exempt_templates=exempt)
        assert verifier.exempt_templates == exempt

    def test_non_exempt_unscoped_still_caught(self) -> None:
        catalog = TemplateCatalog()
        catalog._templates["legit_exempt"] = QueryTemplate(
            name="legit_exempt",
            cypher="RETURN 1",
            parameters=(),
        )
        catalog._templates["sneaky_unscoped"] = QueryTemplate(
            name="sneaky_unscoped",
            cypher="MATCH (n:Service) RETURN n LIMIT 10",
            parameters=(),
        )
        exempt: FrozenSet[str] = frozenset({"legit_exempt"})
        verifier = TenantScopeVerifier(catalog=catalog, exempt_templates=exempt)
        with pytest.raises(TenantTemplateViolationError, match="sneaky_unscoped"):
            verifier.verify_all_templates()


class TestEveryRegisteredTemplateHasTenantScope:

    @pytest.mark.parametrize(
        "template_name",
        sorted(TemplateCatalog().all_templates().keys()),
    )
    def test_template_contains_tenant_scope(
        self, template_name: str,
    ) -> None:
        catalog = TemplateCatalog()
        template = catalog.get(template_name)
        assert template is not None
        assert TenantScopeVerifier.has_tenant_scope(template.cypher), (
            f"Template '{template_name}' is missing tenant_id scope in its "
            f"Cypher query. Every query template must include $tenant_id or "
            f"tenant_id in a WHERE clause."
        )


class TestFulltextUnscopedUnreachableInProduction:

    def test_production_mode_raises_without_tenant_id(self) -> None:
        with pytest.raises(FulltextTenantRequired):
            build_fulltext_fallback_cypher(tenant_id="", deployment_mode="production")

    def test_production_env_var_raises_without_tenant_id(self) -> None:
        original = os.environ.get("DEPLOYMENT_MODE")
        try:
            os.environ["DEPLOYMENT_MODE"] = "production"
            with pytest.raises(FulltextTenantRequired):
                build_fulltext_fallback_cypher(tenant_id="")
        finally:
            if original is None:
                os.environ.pop("DEPLOYMENT_MODE", None)
            else:
                os.environ["DEPLOYMENT_MODE"] = original

    def test_dev_mode_raises_without_tenant_id(self) -> None:
        with pytest.raises(FulltextTenantRequired):
            build_fulltext_fallback_cypher(
                tenant_id="", deployment_mode="dev",
            )

    def test_tenant_id_always_returns_scoped(self) -> None:
        result = build_fulltext_fallback_cypher(
            tenant_id="acme-corp", deployment_mode="production",
        )
        assert "$tenant_id" in result


class TestStartupVerificationBehavior:

    def test_production_mode_aborts_on_unscoped_template(self) -> None:
        catalog = TemplateCatalog()
        catalog._templates["leak_vector"] = QueryTemplate(
            name="leak_vector",
            cypher="MATCH (n:Service) RETURN n",
            parameters=(),
        )
        verifier = TenantScopeVerifier(catalog=catalog)
        with pytest.raises(TenantTemplateViolationError):
            verifier.enforce_startup(deployment_mode="production")

    def test_dev_mode_logs_warning_on_unscoped_template(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        catalog = TemplateCatalog()
        catalog._templates["leak_vector"] = QueryTemplate(
            name="leak_vector",
            cypher="MATCH (n:Service) RETURN n",
            parameters=(),
        )
        verifier = TenantScopeVerifier(catalog=catalog)
        with caplog.at_level(logging.WARNING):
            verifier.enforce_startup(deployment_mode="dev")
        assert any("leak_vector" in record.message for record in caplog.records)

    def test_production_mode_passes_with_clean_catalog(self) -> None:
        catalog = TemplateCatalog()
        verifier = TenantScopeVerifier(catalog=catalog)
        verifier.enforce_startup(deployment_mode="production")
