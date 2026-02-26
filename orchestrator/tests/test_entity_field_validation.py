from __future__ import annotations

import pytest
from pydantic import ValidationError

from orchestrator.app.extraction_models import (
    CallsEdge,
    ConsumesEdge,
    DatabaseNode,
    DeployedInEdge,
    K8sDeploymentNode,
    KafkaTopicNode,
    ProducesEdge,
    ServiceNode,
)


class TestServiceNodeNameValidation:

    def test_valid_kebab_case_name(self) -> None:
        node = ServiceNode(
            id="auth-service",
            name="auth-service",
            language="go",
            framework="gin",
            opentelemetry_enabled=True,
            tenant_id="t1",
        )
        assert node.name == "auth-service"

    def test_valid_name_with_dots_and_digits(self) -> None:
        node = ServiceNode(
            id="auth-v2.0",
            name="auth-v2.0",
            language="go",
            framework="gin",
            opentelemetry_enabled=True,
            tenant_id="t1",
        )
        assert node.name == "auth-v2.0"

    def test_rejects_cypher_semicolon(self) -> None:
        with pytest.raises(ValidationError):
            ServiceNode(
                id="auth; DROP",
                name="auth; DROP",
                language="go",
                framework="gin",
                opentelemetry_enabled=True,
                tenant_id="t1",
            )

    def test_rejects_single_quote(self) -> None:
        with pytest.raises(ValidationError):
            ServiceNode(
                id="auth'injection",
                name="auth'injection",
                language="go",
                framework="gin",
                opentelemetry_enabled=True,
                tenant_id="t1",
            )

    def test_rejects_double_quote(self) -> None:
        with pytest.raises(ValidationError):
            ServiceNode(
                id='auth"injection',
                name='auth"injection',
                language="go",
                framework="gin",
                opentelemetry_enabled=True,
                tenant_id="t1",
            )

    def test_rejects_curly_braces(self) -> None:
        with pytest.raises(ValidationError):
            ServiceNode(
                id="auth{bad}",
                name="auth{bad}",
                language="go",
                framework="gin",
                opentelemetry_enabled=True,
                tenant_id="t1",
            )

    def test_rejects_null_byte(self) -> None:
        with pytest.raises(ValidationError):
            ServiceNode(
                id="auth\x00injected",
                name="auth\x00injected",
                language="go",
                framework="gin",
                opentelemetry_enabled=True,
                tenant_id="t1",
            )

    def test_rejects_backslash(self) -> None:
        with pytest.raises(ValidationError):
            ServiceNode(
                id="auth\\escape",
                name="auth\\escape",
                language="go",
                framework="gin",
                opentelemetry_enabled=True,
                tenant_id="t1",
            )


class TestKafkaTopicNameValidation:

    def test_valid_topic_name(self) -> None:
        node = KafkaTopicNode(
            name="order-events",
            partitions=6,
            retention_ms=604800000,
            tenant_id="t1",
        )
        assert node.name == "order-events"

    def test_rejects_poisoned_topic_name(self) -> None:
        with pytest.raises(ValidationError):
            KafkaTopicNode(
                name="topic'; DROP",
                partitions=6,
                retention_ms=604800000,
                tenant_id="t1",
            )


class TestK8sDeploymentIdValidation:

    def test_valid_deployment_id(self) -> None:
        node = K8sDeploymentNode(
            id="order-deploy-v2",
            namespace="production",
            replicas=3,
            tenant_id="t1",
        )
        assert node.id == "order-deploy-v2"

    def test_rejects_poisoned_deployment_id(self) -> None:
        with pytest.raises(ValidationError):
            K8sDeploymentNode(
                id="deploy{evil: true}",
                namespace="production",
                replicas=3,
                tenant_id="t1",
            )


class TestDatabaseNodeIdValidation:

    def test_valid_database_id(self) -> None:
        node = DatabaseNode(
            id="orders-db",
            type="postgresql",
            tenant_id="t1",
        )
        assert node.id == "orders-db"

    def test_rejects_poisoned_database_id(self) -> None:
        with pytest.raises(ValidationError):
            DatabaseNode(
                id="db'; DELETE FROM",
                type="postgresql",
                tenant_id="t1",
            )


class TestCallsEdgeValidation:

    def test_valid_calls_edge(self) -> None:
        edge = CallsEdge(
            source_service_id="auth-service",
            target_service_id="order-service",
            protocol="grpc",
            tenant_id="test-tenant",
        )
        assert edge.source_service_id == "auth-service"

    def test_accepts_ast_extracted_identifiers(self) -> None:
        edge = CallsEdge(
            source_service_id="::services::gateway",
            target_service_id="http://auth-svc/verify",
            protocol="http",
            tenant_id="test-tenant",
        )
        assert edge.source_service_id == "::services::gateway"

    def test_rejects_single_quote_injection(self) -> None:
        with pytest.raises(ValidationError):
            CallsEdge(
                source_service_id="auth'; DROP",
                target_service_id="order-service",
                protocol="grpc",
                tenant_id="test-tenant",
            )

    def test_rejects_curly_brace_injection(self) -> None:
        with pytest.raises(ValidationError):
            CallsEdge(
                source_service_id="auth-service",
                target_service_id="order{evil}",
                protocol="grpc",
                tenant_id="test-tenant",
            )

    def test_rejects_semicolon_injection(self) -> None:
        with pytest.raises(ValidationError):
            CallsEdge(
                source_service_id="auth;DROP",
                target_service_id="order-service",
                protocol="grpc",
                tenant_id="test-tenant",
            )

    def test_rejects_empty_source(self) -> None:
        with pytest.raises(ValidationError):
            CallsEdge(
                source_service_id="",
                target_service_id="order-service",
                protocol="grpc",
                tenant_id="test-tenant",
            )


class TestProducesEdgeValidation:

    def test_valid_produces_edge(self) -> None:
        edge = ProducesEdge(
            service_id="order-service",
            topic_name="order-events",
            event_schema="OrderCreated",
            tenant_id="test-tenant",
        )
        assert edge.topic_name == "order-events"

    def test_rejects_semicolon_in_service_id(self) -> None:
        with pytest.raises(ValidationError):
            ProducesEdge(
                service_id="svc; DROP",
                topic_name="order-events",
                event_schema="OrderCreated",
                tenant_id="test-tenant",
            )

    def test_rejects_single_quote_in_topic(self) -> None:
        with pytest.raises(ValidationError):
            ProducesEdge(
                service_id="order-service",
                topic_name="topic'injection",
                event_schema="OrderCreated",
                tenant_id="test-tenant",
            )


class TestConsumesEdgeValidation:

    def test_valid_consumes_edge(self) -> None:
        edge = ConsumesEdge(
            service_id="payment-service",
            topic_name="order-events",
            consumer_group="payment-cg",
            tenant_id="test-tenant",
        )
        assert edge.consumer_group == "payment-cg"

    def test_rejects_curly_brace_in_consumer_group(self) -> None:
        with pytest.raises(ValidationError):
            ConsumesEdge(
                service_id="payment-service",
                topic_name="order-events",
                consumer_group="cg{bad}",
                tenant_id="test-tenant",
            )


class TestDeployedInEdgeValidation:

    def test_valid_deployed_in_edge(self) -> None:
        edge = DeployedInEdge(
            service_id="auth-service",
            deployment_id="auth-deploy-v2",
            tenant_id="test-tenant",
        )
        assert edge.deployment_id == "auth-deploy-v2"

    def test_rejects_single_quote_in_deployment_id(self) -> None:
        with pytest.raises(ValidationError):
            DeployedInEdge(
                service_id="auth-service",
                deployment_id="deploy'; DROP",
                tenant_id="test-tenant",
            )

    def test_rejects_backslash_in_service_id(self) -> None:
        with pytest.raises(ValidationError):
            DeployedInEdge(
                service_id="svc\\escape",
                deployment_id="auth-deploy-v2",
                tenant_id="test-tenant",
            )

    def test_rejects_null_byte(self) -> None:
        with pytest.raises(ValidationError):
            DeployedInEdge(
                service_id="svc\x00injected",
                deployment_id="auth-deploy-v2",
                tenant_id="test-tenant",
            )


class TestMaxLengthEnforcement:

    def test_rejects_name_over_253_chars(self) -> None:
        with pytest.raises(ValidationError):
            ServiceNode(
                id="a" * 254,
                name="a" * 254,
                language="go",
                framework="gin",
                opentelemetry_enabled=True,
                tenant_id="t1",
            )
