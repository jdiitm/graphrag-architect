from __future__ import annotations

from typing import Any, List
from unittest.mock import AsyncMock, MagicMock, patch

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
from orchestrator.app.neo4j_client import GraphRepository, cypher_op_for_entity


SAMPLE_SERVICE = ServiceNode(
    id="order-service",
    name="order-service",
    language="go",
    framework="gin",
    opentelemetry_enabled=True,
    tenant_id="test-tenant",
)

SAMPLE_DATABASE = DatabaseNode(id="orders-db", type="postgresql", tenant_id="test-tenant")

SAMPLE_TOPIC = KafkaTopicNode(name="order-events", partitions=6, retention_ms=604800000, tenant_id="test-tenant")

SAMPLE_K8S = K8sDeploymentNode(id="order-deploy", namespace="production", replicas=3, tenant_id="test-tenant")

SAMPLE_CALLS = CallsEdge(
    source_service_id="user-service",
    target_service_id="order-service",
    protocol="http",
)

SAMPLE_PRODUCES = ProducesEdge(
    service_id="order-service",
    topic_name="order-events",
    event_schema="OrderCreated",
)

SAMPLE_CONSUMES = ConsumesEdge(
    service_id="notification-service",
    topic_name="order-events",
    consumer_group="notification-cg",
)

SAMPLE_DEPLOYED_IN = DeployedInEdge(
    service_id="order-service",
    deployment_id="order-deploy",
)


class TestCypherOpForServiceNode:

    def test_returns_merge_on_id(self) -> None:
        query, params = cypher_op_for_entity(SAMPLE_SERVICE)
        assert "MERGE" in query
        assert "{id: $id}" in query
        assert ":Service" in query
        assert params["id"] == "order-service"

    def test_sets_all_properties(self) -> None:
        query, params = cypher_op_for_entity(SAMPLE_SERVICE)
        assert params["name"] == "order-service"
        assert params["language"] == "go"
        assert params["framework"] == "gin"
        assert params["opentelemetry_enabled"] is True


class TestCypherOpForDatabaseNode:

    def test_returns_merge_on_id(self) -> None:
        query, params = cypher_op_for_entity(SAMPLE_DATABASE)
        assert "MERGE" in query
        assert ":Database" in query
        assert params["id"] == "orders-db"
        assert params["type"] == "postgresql"


class TestCypherOpForKafkaTopicNode:

    def test_returns_merge_on_name(self) -> None:
        query, params = cypher_op_for_entity(SAMPLE_TOPIC)
        assert "MERGE" in query
        assert ":KafkaTopic" in query
        assert "{name: $name}" in query
        assert params["name"] == "order-events"
        assert params["partitions"] == 6
        assert params["retention_ms"] == 604800000


class TestCypherOpForK8sDeploymentNode:

    def test_returns_merge_on_id(self) -> None:
        query, params = cypher_op_for_entity(SAMPLE_K8S)
        assert "MERGE" in query
        assert ":K8sDeployment" in query
        assert params["id"] == "order-deploy"
        assert params["namespace"] == "production"
        assert params["replicas"] == 3


class TestCypherOpForCallsEdge:

    def test_returns_match_merge_pattern(self) -> None:
        query, params = cypher_op_for_entity(SAMPLE_CALLS)
        assert "MATCH" in query
        assert "MERGE" in query
        assert ":CALLS" in query
        assert params["source_service_id"] == "user-service"
        assert params["target_service_id"] == "order-service"
        assert params["protocol"] == "http"


class TestCypherOpForProducesEdge:

    def test_returns_match_merge_pattern(self) -> None:
        query, params = cypher_op_for_entity(SAMPLE_PRODUCES)
        assert "MATCH" in query
        assert "MERGE" in query
        assert ":PRODUCES" in query
        assert params["service_id"] == "order-service"
        assert params["topic_name"] == "order-events"
        assert params["event_schema"] == "OrderCreated"


class TestCypherOpForConsumesEdge:

    def test_returns_match_merge_pattern(self) -> None:
        query, params = cypher_op_for_entity(SAMPLE_CONSUMES)
        assert "MATCH" in query
        assert "MERGE" in query
        assert ":CONSUMES" in query
        assert params["service_id"] == "notification-service"
        assert params["topic_name"] == "order-events"
        assert params["consumer_group"] == "notification-cg"


class TestCypherOpForDeployedInEdge:

    def test_returns_match_merge_pattern(self) -> None:
        query, params = cypher_op_for_entity(SAMPLE_DEPLOYED_IN)
        assert "MATCH" in query
        assert "MERGE" in query
        assert ":DEPLOYED_IN" in query
        assert params["service_id"] == "order-service"
        assert params["deployment_id"] == "order-deploy"


class TestCypherOpUnsupported:

    def test_raises_for_unknown_type(self) -> None:
        with pytest.raises(TypeError, match="Unsupported entity type"):
            cypher_op_for_entity({"not": "a model"})


def _mock_driver() -> MagicMock:
    mock_tx = AsyncMock()
    mock_tx.run = AsyncMock()

    mock_session = AsyncMock()

    async def _execute_write_side_effect(fn, **kw):
        return await fn(mock_tx, **kw)

    mock_session.execute_write = AsyncMock(side_effect=_execute_write_side_effect)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_driver = MagicMock()
    mock_driver.session.return_value = mock_session

    return mock_driver, mock_session, mock_tx


class TestCommitTopologyMixed:

    @pytest.mark.asyncio
    async def test_merges_nodes_before_edges(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)
        entities: List[Any] = [
            SAMPLE_CALLS,
            SAMPLE_SERVICE,
            ServiceNode(
                id="user-service",
                name="user-service",
                language="python",
                framework="fastapi",
                opentelemetry_enabled=False,
                tenant_id="test-tenant",
            ),
        ]

        await repo.commit_topology(entities)

        run_calls = tx.run.call_args_list
        assert len(run_calls) == 2
        node_call = run_calls[0]
        assert "UNWIND" in node_call.args[0]
        assert ":Service" in node_call.args[0]
        assert len(node_call.kwargs["batch"]) == 2
        edge_call = run_calls[1]
        assert ":CALLS" in edge_call.args[0]

    @pytest.mark.asyncio
    async def test_commits_all_entity_types(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)
        entities: List[Any] = [
            SAMPLE_SERVICE,
            SAMPLE_DATABASE,
            SAMPLE_TOPIC,
            SAMPLE_K8S,
            SAMPLE_CALLS,
            SAMPLE_PRODUCES,
            SAMPLE_CONSUMES,
            SAMPLE_DEPLOYED_IN,
        ]

        await repo.commit_topology(entities)

        assert tx.run.call_count == 8
        for c in tx.run.call_args_list:
            assert "UNWIND" in c.args[0]


class TestCommitTopologyEmpty:

    @pytest.mark.asyncio
    async def test_no_driver_interaction_on_empty(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)

        await repo.commit_topology([])

        session.execute_write.assert_not_called()


class TestCommitTopologyRollback:

    @pytest.mark.asyncio
    async def test_propagates_neo4j_error(self) -> None:
        from neo4j.exceptions import Neo4jError

        driver, session, tx = _mock_driver()
        tx.run = AsyncMock(side_effect=Neo4jError("constraint violation"))
        repo = GraphRepository(driver)

        with pytest.raises(Neo4jError):
            await repo.commit_topology([SAMPLE_SERVICE])


class TestWriteBatchesUnknownEntityType:

    @pytest.mark.asyncio
    async def test_raises_on_missing_unwind_query(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)
        with pytest.raises(TypeError, match="No UNWIND query registered"):
            await repo._write_batches(dict, [{"id": "bogus"}])


class TestCypherIdentifierValidation:

    @pytest.mark.asyncio
    async def test_create_vector_index_rejects_injection_in_label(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)
        with pytest.raises(ValueError, match="Invalid Cypher identifier"):
            await repo.create_vector_index(label="Service DETACH DELETE n //")

    @pytest.mark.asyncio
    async def test_create_vector_index_rejects_injection_in_index_name(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)
        with pytest.raises(ValueError, match="Invalid Cypher identifier"):
            await repo.create_vector_index(index_name="idx; DROP INDEX")

    @pytest.mark.asyncio
    async def test_create_vector_index_rejects_injection_in_property(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)
        with pytest.raises(ValueError, match="Invalid Cypher identifier"):
            await repo.create_vector_index(property_name="emb}) RETURN n //")

    @pytest.mark.asyncio
    async def test_upsert_embeddings_rejects_injection_in_label(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)
        with pytest.raises(ValueError, match="Invalid Cypher identifier"):
            await repo.upsert_embeddings(
                label="Service}) DETACH DELETE n //",
                id_field="id",
                embeddings=[{"id": "x", "embedding": [0.1]}],
            )

    @pytest.mark.asyncio
    async def test_upsert_embeddings_rejects_injection_in_id_field(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)
        with pytest.raises(ValueError, match="Invalid Cypher identifier"):
            await repo.upsert_embeddings(
                label="Service",
                id_field="id: 'x'}) SET n.hacked = true //",
                embeddings=[{"id": "x", "embedding": [0.1]}],
            )

    @pytest.mark.asyncio
    async def test_valid_identifiers_accepted(self) -> None:
        driver, session, tx = _mock_driver()
        mock_session_obj = AsyncMock()
        mock_session_obj.run = AsyncMock()
        mock_session_obj.execute_write = AsyncMock(
            side_effect=lambda fn, **kw: fn(tx, **kw)
        )
        mock_session_obj.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_obj.__aexit__ = AsyncMock(return_value=False)
        driver.session.return_value = mock_session_obj

        repo = GraphRepository(driver)
        await repo.create_vector_index(
            index_name="my_index", label="Service",
            property_name="embedding", dimensions=1536,
        )
        mock_session_obj.run.assert_called_once()


class TestGraphRepositoryTenantDatabase:

    @pytest.mark.asyncio
    async def test_session_uses_specified_database(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver, database="acme_db")
        await repo.commit_topology([SAMPLE_SERVICE])
        driver.session.assert_called_with(database="acme_db")

    @pytest.mark.asyncio
    async def test_session_omits_database_when_none(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)
        await repo.commit_topology([SAMPLE_SERVICE])
        driver.session.assert_called_with()


class TestCommitTopologyIdempotent:

    @pytest.mark.asyncio
    async def test_duplicate_entities_both_merged(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)

        await repo.commit_topology([SAMPLE_SERVICE, SAMPLE_SERVICE])

        assert tx.run.call_count == 1
        batch = tx.run.call_args.kwargs["batch"]
        assert len(batch) == 2
