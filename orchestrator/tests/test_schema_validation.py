from __future__ import annotations

from typing import Any, List
from unittest.mock import AsyncMock, patch

import pytest

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
from orchestrator.app.schema_validation import validate_topology


AUTH_SERVICE = ServiceNode(
    id="auth-service",
    name="auth-service",
    language="go",
    framework="gin",
    opentelemetry_enabled=True,
)

ORDER_SERVICE = ServiceNode(
    id="order-service",
    name="order-service",
    language="python",
    framework="fastapi",
    opentelemetry_enabled=False,
)

ORDERS_DB = DatabaseNode(id="orders-db", type="postgresql")

ORDER_EVENTS_TOPIC = KafkaTopicNode(
    name="order-events", partitions=6, retention_ms=604800000
)

ORDER_DEPLOY = K8sDeploymentNode(
    id="order-deploy", namespace="production", replicas=3
)

VALID_CALLS = CallsEdge(
    source_service_id="auth-service",
    target_service_id="order-service",
    protocol="http",
)

VALID_PRODUCES = ProducesEdge(
    service_id="order-service",
    topic_name="order-events",
    event_schema="OrderCreated",
)

VALID_CONSUMES = ConsumesEdge(
    service_id="auth-service",
    topic_name="order-events",
    consumer_group="auth-cg",
)

VALID_DEPLOYED_IN = DeployedInEdge(
    service_id="order-service",
    deployment_id="order-deploy",
)


class TestValidateTopologyHappyPath:

    def test_valid_nodes_only(self) -> None:
        entities: List[Any] = [AUTH_SERVICE, ORDER_SERVICE, ORDERS_DB]
        errors = validate_topology(entities)
        assert errors == []

    def test_valid_full_topology(self) -> None:
        entities: List[Any] = [
            AUTH_SERVICE,
            ORDER_SERVICE,
            ORDERS_DB,
            ORDER_EVENTS_TOPIC,
            ORDER_DEPLOY,
            VALID_CALLS,
            VALID_PRODUCES,
            VALID_CONSUMES,
            VALID_DEPLOYED_IN,
        ]
        errors = validate_topology(entities)
        assert errors == []

    def test_empty_entities(self) -> None:
        errors = validate_topology([])
        assert errors == []


class TestValidateTopologyUnknownTypes:

    def test_dict_rejected(self) -> None:
        entities: List[Any] = [AUTH_SERVICE, {"bad": "entity"}]
        errors = validate_topology(entities)
        assert len(errors) == 1
        assert "dict" in errors[0].lower() or "Unknown" in errors[0]

    def test_string_rejected(self) -> None:
        entities: List[Any] = [AUTH_SERVICE, "not-an-entity"]
        errors = validate_topology(entities)
        assert len(errors) == 1


class TestValidateTopologyCallsEdge:

    def test_missing_source_service(self) -> None:
        dangling = CallsEdge(
            source_service_id="ghost-service",
            target_service_id="order-service",
            protocol="grpc",
        )
        entities: List[Any] = [ORDER_SERVICE, dangling]
        errors = validate_topology(entities)
        assert len(errors) == 1
        assert "ghost-service" in errors[0]

    def test_missing_target_service(self) -> None:
        dangling = CallsEdge(
            source_service_id="auth-service",
            target_service_id="ghost-service",
            protocol="http",
        )
        entities: List[Any] = [AUTH_SERVICE, dangling]
        errors = validate_topology(entities)
        assert len(errors) == 1
        assert "ghost-service" in errors[0]


class TestValidateTopologyProducesEdge:

    def test_missing_service(self) -> None:
        dangling = ProducesEdge(
            service_id="ghost-service",
            topic_name="order-events",
            event_schema="OrderCreated",
        )
        entities: List[Any] = [ORDER_EVENTS_TOPIC, dangling]
        errors = validate_topology(entities)
        assert len(errors) == 1
        assert "ghost-service" in errors[0]

    def test_missing_topic(self) -> None:
        dangling = ProducesEdge(
            service_id="order-service",
            topic_name="ghost-topic",
            event_schema="OrderCreated",
        )
        entities: List[Any] = [ORDER_SERVICE, dangling]
        errors = validate_topology(entities)
        assert len(errors) == 1
        assert "ghost-topic" in errors[0]


class TestValidateTopologyConsumesEdge:

    def test_missing_service_and_topic(self) -> None:
        dangling = ConsumesEdge(
            service_id="ghost-service",
            topic_name="ghost-topic",
            consumer_group="cg",
        )
        errors = validate_topology([dangling])
        assert len(errors) == 2


class TestValidateTopologyDeployedInEdge:

    def test_missing_service_and_deployment(self) -> None:
        dangling = DeployedInEdge(
            service_id="ghost-service",
            deployment_id="ghost-deploy",
        )
        errors = validate_topology([dangling])
        assert len(errors) == 2


class TestValidateTopologyMixed:

    def test_multiple_errors_accumulated(self) -> None:
        entities: List[Any] = [
            AUTH_SERVICE,
            {"bad": True},
            CallsEdge(
                source_service_id="auth-service",
                target_service_id="missing-svc",
                protocol="http",
            ),
            DeployedInEdge(
                service_id="missing-svc",
                deployment_id="missing-deploy",
            ),
        ]
        errors = validate_topology(entities)
        assert len(errors) >= 3


class TestRouteValidation:

    def test_routes_to_commit_when_no_errors(self) -> None:
        from orchestrator.app.graph_builder import route_validation

        state = {
            "extraction_errors": [],
            "validation_retries": 0,
        }
        assert route_validation(state) == "commit_to_neo4j"

    def test_routes_to_fix_when_errors_under_max(self) -> None:
        from orchestrator.app.graph_builder import route_validation

        state = {
            "extraction_errors": ["some error"],
            "validation_retries": 1,
        }
        assert route_validation(state) == "fix_extraction_errors"

    def test_routes_to_commit_when_retries_exhausted(self) -> None:
        from orchestrator.app.graph_builder import route_validation

        state = {
            "extraction_errors": ["persistent error"],
            "validation_retries": 3,
        }
        assert route_validation(state) == "commit_to_neo4j"


class TestValidateExtractedSchemaNode:

    def test_populates_errors_from_validation(self) -> None:
        from orchestrator.app.graph_builder import validate_extracted_schema

        state = {
            "extracted_nodes": [AUTH_SERVICE, {"bad": True}],
        }
        result = validate_extracted_schema(state)
        assert len(result["extraction_errors"]) >= 1

    def test_no_errors_on_valid_entities(self) -> None:
        from orchestrator.app.graph_builder import validate_extracted_schema

        state = {
            "extracted_nodes": [AUTH_SERVICE, ORDER_SERVICE, VALID_CALLS],
        }
        result = validate_extracted_schema(state)
        assert result["extraction_errors"] == []


class TestFixExtractionErrorsNode:

    @pytest.mark.asyncio
    async def test_increments_retry_counter(self) -> None:
        from orchestrator.app.graph_builder import fix_extraction_errors

        state = {
            "raw_files": [
                {"path": "main.go", "content": "package main"},
            ],
            "extracted_nodes": [AUTH_SERVICE, {"bad": True}],
            "extraction_errors": ["Unknown entity type: dict"],
            "validation_retries": 1,
        }

        with (
            patch("orchestrator.app.graph_builder.ExtractionConfig"),
            patch(
                "orchestrator.app.graph_builder.ServiceExtractor"
            ) as mock_cls,
        ):
            from orchestrator.app.extraction_models import (
                ServiceExtractionResult,
            )

            mock_extractor = mock_cls.return_value
            mock_extractor.extract_all = AsyncMock(
                return_value=ServiceExtractionResult(
                    services=[AUTH_SERVICE], calls=[]
                )
            )

            result = await fix_extraction_errors(state)

        assert result["validation_retries"] == 2
        assert len(result["extraction_errors"]) == 0

    @pytest.mark.asyncio
    async def test_returns_reextracted_entities(self) -> None:
        from orchestrator.app.graph_builder import fix_extraction_errors

        state = {
            "raw_files": [
                {"path": "main.go", "content": "package main"},
            ],
            "extracted_nodes": [{"bad": True}],
            "extraction_errors": ["Unknown entity type: dict"],
            "validation_retries": 0,
        }

        with (
            patch("orchestrator.app.graph_builder.ExtractionConfig"),
            patch(
                "orchestrator.app.graph_builder.ServiceExtractor"
            ) as mock_cls,
        ):
            from orchestrator.app.extraction_models import (
                ServiceExtractionResult,
            )

            mock_extractor = mock_cls.return_value
            mock_extractor.extract_all = AsyncMock(
                return_value=ServiceExtractionResult(
                    services=[AUTH_SERVICE, ORDER_SERVICE],
                    calls=[VALID_CALLS],
                )
            )

            result = await fix_extraction_errors(state)

        assert len(result["extracted_nodes"]) == 3
        assert result["validation_retries"] == 1

    @pytest.mark.asyncio
    async def test_preserves_manifest_entities_during_fix_cycle(self) -> None:
        from orchestrator.app.graph_builder import fix_extraction_errors

        state = {
            "raw_files": [
                {"path": "main.go", "content": "package main"},
                {"path": "deploy.yaml", "content": "kind: Deployment"},
            ],
            "extracted_nodes": [
                AUTH_SERVICE,
                ORDER_DEPLOY,
                ORDER_EVENTS_TOPIC,
                {"bad": True},
            ],
            "extraction_errors": ["Unknown entity type: dict"],
            "validation_retries": 0,
        }

        with (
            patch("orchestrator.app.graph_builder.ExtractionConfig"),
            patch(
                "orchestrator.app.graph_builder.ServiceExtractor"
            ) as mock_cls,
        ):
            from orchestrator.app.extraction_models import (
                ServiceExtractionResult,
            )

            mock_extractor = mock_cls.return_value
            mock_extractor.extract_all = AsyncMock(
                return_value=ServiceExtractionResult(
                    services=[AUTH_SERVICE], calls=[VALID_CALLS]
                )
            )

            result = await fix_extraction_errors(state)

        extracted = result["extracted_nodes"]
        k8s_nodes = [e for e in extracted if isinstance(e, K8sDeploymentNode)]
        kafka_nodes = [e for e in extracted if isinstance(e, KafkaTopicNode)]
        assert len(k8s_nodes) == 1
        assert k8s_nodes[0].id == "order-deploy"
        assert len(kafka_nodes) == 1
        assert kafka_nodes[0].name == "order-events"

    @pytest.mark.asyncio
    async def test_replaces_only_llm_entities_during_fix(self) -> None:
        from orchestrator.app.graph_builder import fix_extraction_errors

        old_service = ServiceNode(
            id="stale-service",
            name="stale-service",
            language="rust",
            framework="actix",
            opentelemetry_enabled=False,
            confidence=0.7,
        )
        state = {
            "raw_files": [
                {"path": "main.go", "content": "package main"},
            ],
            "extracted_nodes": [
                old_service,
                ORDER_DEPLOY,
                ORDER_EVENTS_TOPIC,
            ],
            "extraction_errors": ["dangling edge"],
            "validation_retries": 1,
        }

        with (
            patch("orchestrator.app.graph_builder.ExtractionConfig"),
            patch(
                "orchestrator.app.graph_builder.ServiceExtractor"
            ) as mock_cls,
        ):
            from orchestrator.app.extraction_models import (
                ServiceExtractionResult,
            )

            mock_extractor = mock_cls.return_value
            mock_extractor.extract_all = AsyncMock(
                return_value=ServiceExtractionResult(
                    services=[AUTH_SERVICE, ORDER_SERVICE],
                    calls=[VALID_CALLS],
                )
            )

            result = await fix_extraction_errors(state)

        extracted = result["extracted_nodes"]
        service_ids = [
            e.id for e in extracted if isinstance(e, ServiceNode)
        ]
        assert "stale-service" not in service_ids
        assert "auth-service" in service_ids
        assert "order-service" in service_ids
        assert any(isinstance(e, K8sDeploymentNode) for e in extracted)
        assert any(isinstance(e, KafkaTopicNode) for e in extracted)
        assert result["validation_retries"] == 2

    @pytest.mark.asyncio
    async def test_preserves_manifests_with_no_prior_llm_entities(self) -> None:
        from orchestrator.app.graph_builder import fix_extraction_errors

        state = {
            "raw_files": [
                {"path": "main.go", "content": "package main"},
            ],
            "extracted_nodes": [
                ORDER_DEPLOY,
                ORDER_EVENTS_TOPIC,
            ],
            "extraction_errors": ["some error"],
            "validation_retries": 0,
        }

        with (
            patch("orchestrator.app.graph_builder.ExtractionConfig"),
            patch(
                "orchestrator.app.graph_builder.ServiceExtractor"
            ) as mock_cls,
        ):
            from orchestrator.app.extraction_models import (
                ServiceExtractionResult,
            )

            mock_extractor = mock_cls.return_value
            mock_extractor.extract_all = AsyncMock(
                return_value=ServiceExtractionResult(
                    services=[AUTH_SERVICE], calls=[]
                )
            )

            result = await fix_extraction_errors(state)

        extracted = result["extracted_nodes"]
        assert len(extracted) == 3
        assert any(isinstance(e, K8sDeploymentNode) for e in extracted)
        assert any(isinstance(e, KafkaTopicNode) for e in extracted)
        assert any(isinstance(e, ServiceNode) for e in extracted)
