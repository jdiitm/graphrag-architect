from __future__ import annotations

import pytest

from orchestrator.app.query_templates import (
    QueryTemplate,
    TemplateMatch,
    TemplateCatalog,
    match_template,
)


class TestTemplateCatalog:

    def test_catalog_has_blast_radius(self) -> None:
        catalog = TemplateCatalog()
        template = catalog.get("blast_radius")
        assert template is not None
        assert "$name" in template.cypher

    def test_catalog_has_dependency_count(self) -> None:
        catalog = TemplateCatalog()
        template = catalog.get("dependency_count")
        assert template is not None
        assert "$limit" in template.cypher

    def test_catalog_has_service_neighbors(self) -> None:
        catalog = TemplateCatalog()
        template = catalog.get("service_neighbors")
        assert template is not None
        assert "$name" in template.cypher

    def test_catalog_has_topic_consumers(self) -> None:
        catalog = TemplateCatalog()
        template = catalog.get("topic_consumers")
        assert template is not None
        assert "$topic_name" in template.cypher

    def test_get_unknown_returns_none(self) -> None:
        catalog = TemplateCatalog()
        assert catalog.get("nonexistent") is None

    def test_all_templates_have_readonly_cypher(self) -> None:
        from orchestrator.app.cypher_validator import validate_cypher_readonly
        catalog = TemplateCatalog()
        for name, template in catalog.all_templates().items():
            cypher_with_fake_params = template.cypher
            for param in template.parameters:
                cypher_with_fake_params = cypher_with_fake_params.replace(
                    f"${param}", "'test'"
                )
            validate_cypher_readonly(cypher_with_fake_params)


class TestMatchTemplate:

    def test_matches_blast_radius_query(self) -> None:
        result = match_template(
            "What is the blast radius if auth-service fails?"
        )
        assert result is not None
        assert result.template_name == "blast_radius"
        assert "name" in result.params

    def test_matches_dependency_count_query(self) -> None:
        result = match_template(
            "What are the most critical services by dependency count?"
        )
        assert result is not None
        assert result.template_name == "dependency_count"

    def test_matches_service_neighbors_query(self) -> None:
        result = match_template("What does order-service call?")
        assert result is not None
        assert result.template_name == "service_neighbors"
        assert "name" in result.params

    def test_matches_topic_consumers_query(self) -> None:
        result = match_template(
            "Which services consume from the orders-topic?"
        )
        assert result is not None
        assert result.template_name == "topic_consumers"
        assert "topic_name" in result.params

    def test_no_match_returns_none(self) -> None:
        result = match_template(
            "Tell me a joke about distributed systems"
        )
        assert result is None

    def test_intent_matches_but_entity_extraction_fails_returns_none(self) -> None:
        result = match_template("What is the blast radius?")
        assert result is None, (
            "Expected None when intent matches but required params are missing, "
            f"got {result}"
        )

    def test_matches_topic_producers_query(self) -> None:
        result = match_template(
            "Which services produce to the order-events topic?"
        )
        assert result is not None
        assert result.template_name == "topic_producers"
        assert "topic_name" in result.params

    def test_matches_service_deployments_query(self) -> None:
        result = match_template(
            "Where is auth-service deployed?"
        )
        assert result is not None
        assert result.template_name == "service_deployments"
        assert result.params.get("name") == "auth-service"

    def test_matches_cross_team_dependencies(self) -> None:
        result = match_template(
            "Show me all cross-team dependencies"
        )
        assert result is not None
        assert result.template_name == "cross_team_dependencies"

    def test_all_templates_use_parameters_not_interpolation(self) -> None:
        catalog = TemplateCatalog()
        for name, template in catalog.all_templates().items():
            for param in template.parameters:
                assert f"${param}" in template.cypher, (
                    f"Template {name} should use ${param} parameter"
                )
            assert "f'" not in template.cypher, (
                f"Template {name} should not use f-string interpolation"
            )


class TestAclCoverageValidation:

    def test_single_match_with_acl_passes(self) -> None:
        from orchestrator.app.cypher_ast import inject_acl_all_scopes, validate_acl_coverage
        cypher = "MATCH (n:Service) RETURN n"
        acl = "n.team_owner = $acl_team"
        injected = inject_acl_all_scopes(cypher, acl)
        assert validate_acl_coverage(injected, "$acl_team")

    def test_multi_match_all_covered(self) -> None:
        from orchestrator.app.cypher_ast import inject_acl_all_scopes, validate_acl_coverage
        cypher = "MATCH (a:Service) MATCH (b:Service) RETURN a, b"
        acl = "a.team_owner = $acl_team"
        injected = inject_acl_all_scopes(cypher, acl)
        assert validate_acl_coverage(injected, "$acl_team")

    def test_uninjected_cypher_fails_validation(self) -> None:
        from orchestrator.app.cypher_ast import validate_acl_coverage
        cypher = "MATCH (n:Service) RETURN n"
        assert not validate_acl_coverage(cypher, "$acl_team")

    def test_subquery_coverage(self) -> None:
        from orchestrator.app.cypher_ast import inject_acl_all_scopes, validate_acl_coverage
        cypher = "MATCH (a:Service) CALL { MATCH (b:Service) RETURN b } RETURN a, b"
        acl = "a.team_owner = $acl_team"
        injected = inject_acl_all_scopes(cypher, acl)
        assert validate_acl_coverage(injected, "$acl_team")
