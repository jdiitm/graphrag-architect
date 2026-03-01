from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.config import HotTargetConfig
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
from orchestrator.app.neo4j_client import (
    GraphRepository,
    cypher_op_for_entity,
    split_hot_targets,
)


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
    tenant_id="test-tenant",
)

SAMPLE_PRODUCES = ProducesEdge(
    service_id="order-service",
    topic_name="order-events",
    event_schema="OrderCreated",
    tenant_id="test-tenant",
)

SAMPLE_CONSUMES = ConsumesEdge(
    service_id="notification-service",
    topic_name="order-events",
    consumer_group="notification-cg",
    tenant_id="test-tenant",
)

SAMPLE_DEPLOYED_IN = DeployedInEdge(
    service_id="order-service",
    deployment_id="order-deploy",
    tenant_id="test-tenant",
)


class TestCypherOpForServiceNode:

    def test_returns_merge_on_id(self) -> None:
        query, params = cypher_op_for_entity(SAMPLE_SERVICE)
        assert "MERGE" in query
        assert "id: $id" in query
        assert "tenant_id: $tenant_id" in query
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
        assert "name: $name" in query
        assert "tenant_id: $tenant_id" in query
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
        unwind_calls = [c for c in tx.run.call_args_list if "UNWIND" in c.args[0]]
        assert len(unwind_calls) == 8
        degree_calls = [c for c in tx.run.call_args_list if "n.degree" in c.args[0]]
        assert len(degree_calls) == 0


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
    async def test_upsert_embeddings_scopes_by_tenant_id(self) -> None:
        driver, _, tx = _mock_driver()
        mock_session_obj = AsyncMock()

        async def _exec_write(fn, **kw):
            return await fn(tx, **kw)

        mock_session_obj.execute_write = _exec_write
        mock_session_obj.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_obj.__aexit__ = AsyncMock(return_value=False)
        driver.session.return_value = mock_session_obj

        repo = GraphRepository(driver)
        await repo.upsert_embeddings(
            label="Service",
            id_field="id",
            embeddings=[{"id": "svc-1", "embedding": [0.1, 0.2]}],
            tenant_id="acme",
        )

        call_args = tx.run.call_args
        cypher_used = call_args.args[0] if call_args.args else ""
        assert "tenant_id" in cypher_used.lower(), (
            "upsert_embeddings must scope MATCH by tenant_id, "
            f"got query: {cypher_used}"
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
        from neo4j import WRITE_ACCESS
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver, database="acme_db")
        await repo.commit_topology([SAMPLE_SERVICE])
        driver.session.assert_called_with(
            database="acme_db", default_access_mode=WRITE_ACCESS,
        )

    @pytest.mark.asyncio
    async def test_session_omits_database_when_none(self) -> None:
        from neo4j import WRITE_ACCESS
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)
        await repo.commit_topology([SAMPLE_SERVICE])
        driver.session.assert_called_with(default_access_mode=WRITE_ACCESS)


class TestReadReplicaRouting:

    @pytest.mark.asyncio
    async def test_read_operations_use_read_access(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)
        await repo.read_topology(label="Service", tenant_id="test-tenant")
        call_kwargs = driver.session.call_args
        if call_kwargs.kwargs:
            access = call_kwargs.kwargs.get("default_access_mode")
        else:
            access = None
        from neo4j import READ_ACCESS
        assert access == READ_ACCESS

    @pytest.mark.asyncio
    async def test_write_operations_use_write_access(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)
        await repo.commit_topology([SAMPLE_SERVICE])
        call_kwargs = driver.session.call_args
        if call_kwargs.kwargs:
            access = call_kwargs.kwargs.get("default_access_mode")
        else:
            access = None
        from neo4j import WRITE_ACCESS
        assert access == WRITE_ACCESS


class TestEnsureTombstoneIndex:

    @pytest.mark.asyncio
    async def test_creates_index_for_all_four_relationship_types(self) -> None:
        driver, session, tx = _mock_driver()
        mock_session_obj = AsyncMock()
        mock_session_obj.run = AsyncMock()
        mock_session_obj.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_obj.__aexit__ = AsyncMock(return_value=False)
        driver.session.return_value = mock_session_obj

        repo = GraphRepository(driver)
        await repo.ensure_tombstone_index()

        calls = mock_session_obj.run.call_args_list
        cypher_statements = [c.args[0] for c in calls]
        assert len(cypher_statements) == 4, (
            f"Expected 4 index statements, got {len(cypher_statements)}: "
            f"{cypher_statements}"
        )

        expected_rel_types = {"CALLS", "PRODUCES", "CONSUMES", "DEPLOYED_IN"}
        found_rel_types: set[str] = set()
        for stmt in cypher_statements:
            assert "RANGE INDEX" in stmt, (
                f"Expected RANGE INDEX in statement: {stmt}"
            )
            assert "tombstoned_at" in stmt, (
                f"Expected tombstoned_at in statement: {stmt}"
            )
            for rel_type in expected_rel_types:
                if f":{rel_type}]" in stmt:
                    found_rel_types.add(rel_type)

        assert found_rel_types == expected_rel_types, (
            f"Missing relationship types: {expected_rel_types - found_rel_types}"
        )


class TestCommitTopologyIdempotent:

    @pytest.mark.asyncio
    async def test_duplicate_entities_both_merged(self) -> None:
        driver, session, tx = _mock_driver()
        repo = GraphRepository(driver)

        await repo.commit_topology([SAMPLE_SERVICE, SAMPLE_SERVICE])

        assert tx.run.call_count == 1
        unwind_calls = [c for c in tx.run.call_args_list if "UNWIND" in c.args[0]]
        assert len(unwind_calls) == 1
        batch = unwind_calls[0].kwargs["batch"]
        assert len(batch) == 2


class TestHotTargetConfig:

    def test_defaults(self) -> None:
        cfg = HotTargetConfig()
        assert cfg.hot_target_threshold == 10
        assert cfg.hot_target_max_concurrent == 1

    def test_from_env_overrides(self) -> None:
        with patch.dict("os.environ", {
            "HOT_TARGET_THRESHOLD": "5",
            "HOT_TARGET_MAX_CONCURRENT": "2",
        }):
            cfg = HotTargetConfig.from_env()
            assert cfg.hot_target_threshold == 5
            assert cfg.hot_target_max_concurrent == 2

    def test_from_env_uses_defaults(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            cfg = HotTargetConfig.from_env()
            assert cfg.hot_target_threshold == 10
            assert cfg.hot_target_max_concurrent == 1

    def test_frozen(self) -> None:
        cfg = HotTargetConfig()
        with pytest.raises(FrozenInstanceError):
            cfg.hot_target_threshold = 999

    def test_from_env_clamps_zero_max_concurrent(self) -> None:
        with patch.dict("os.environ", {
            "HOT_TARGET_MAX_CONCURRENT": "0",
        }):
            cfg = HotTargetConfig.from_env()
            assert cfg.hot_target_max_concurrent >= 1

    def test_from_env_clamps_zero_threshold(self) -> None:
        with patch.dict("os.environ", {
            "HOT_TARGET_THRESHOLD": "0",
        }):
            cfg = HotTargetConfig.from_env()
            assert cfg.hot_target_threshold >= 1

    def test_direct_construction_clamps_zero_values(self) -> None:
        cfg = HotTargetConfig(
            hot_target_max_concurrent=0,
            hot_target_threshold=0,
        )
        assert cfg.hot_target_max_concurrent >= 1
        assert cfg.hot_target_threshold >= 1

    def test_from_env_clamps_negative_values(self) -> None:
        with patch.dict("os.environ", {
            "HOT_TARGET_MAX_CONCURRENT": "-5",
            "HOT_TARGET_THRESHOLD": "-10",
        }):
            cfg = HotTargetConfig.from_env()
            assert cfg.hot_target_max_concurrent >= 1
            assert cfg.hot_target_threshold >= 1


class TestSplitHotTargets:

    def test_no_hot_targets_returns_all_regular(self) -> None:
        edges = [SAMPLE_CALLS]
        regular, hot = split_hot_targets(edges, threshold=10)
        assert regular == edges
        assert hot == []

    def test_splits_edges_with_hot_target(self) -> None:
        hot_edges = [
            CallsEdge(
                source_service_id=f"svc-{i}",
                target_service_id="auth-service",
                protocol="http",
                tenant_id="test-tenant",
            )
            for i in range(12)
        ]
        cold_edge = CallsEdge(
            source_service_id="unique-src",
            target_service_id="unique-target",
            protocol="grpc",
            tenant_id="test-tenant",
        )
        all_edges = hot_edges + [cold_edge]

        regular, hot = split_hot_targets(all_edges, threshold=10)

        assert len(hot) == 12
        assert len(regular) == 1
        assert all(e.target_service_id == "auth-service" for e in hot)
        assert regular[0].target_service_id == "unique-target"

    def test_empty_edges_returns_empty(self) -> None:
        regular, hot = split_hot_targets([], threshold=10)
        assert regular == []
        assert hot == []

    def test_threshold_boundary_exact(self) -> None:
        edges = [
            CallsEdge(
                source_service_id=f"svc-{i}",
                target_service_id="hub-node",
                protocol="http",
                tenant_id="test-tenant",
            )
            for i in range(10)
        ]
        regular_below, hot_below = split_hot_targets(edges, threshold=11)
        assert len(regular_below) == 10
        assert len(hot_below) == 0

        regular_at, hot_at = split_hot_targets(edges, threshold=10)
        assert len(regular_at) == 0
        assert len(hot_at) == 10

    def test_multiple_hot_targets(self) -> None:
        edges_a = [
            ConsumesEdge(
                service_id=f"consumer-{i}",
                topic_name="hot-topic-a",
                consumer_group=f"cg-{i}",
                tenant_id="test-tenant",
            )
            for i in range(5)
        ]
        edges_b = [
            ConsumesEdge(
                service_id=f"consumer-{i}",
                topic_name="hot-topic-b",
                consumer_group=f"cg-{i}",
                tenant_id="test-tenant",
            )
            for i in range(5)
        ]
        cold = [
            DeployedInEdge(
                service_id="lone-svc",
                deployment_id="lone-deploy",
                tenant_id="test-tenant",
            )
        ]

        regular, hot = split_hot_targets(
            edges_a + edges_b + cold, threshold=5,
        )
        assert len(hot) == 10
        assert len(regular) == 1


class TestBatchedCommitHotTargetSerialization:

    @pytest.mark.asyncio
    async def test_hot_edges_written_in_batched_unwind(self) -> None:
        driver, _, tx = _mock_driver()
        hot_config = HotTargetConfig(
            hot_target_threshold=2, hot_target_max_concurrent=1,
        )
        repo = GraphRepository(driver, hot_target_config=hot_config)

        hot_edges = [
            CallsEdge(
                source_service_id=f"svc-{i}",
                target_service_id="shared-hub",
                protocol="http",
                tenant_id="test-tenant",
            )
            for i in range(3)
        ]
        node = ServiceNode(
            id="svc-0", name="svc-0", language="go",
            framework="gin", opentelemetry_enabled=False,
            tenant_id="test-tenant",
        )

        await repo.commit_topology([node] + hot_edges)

        calls = tx.run.call_args_list
        node_calls = [
            c for c in calls if "MERGE (n:" in c.args[0]
        ]
        edge_calls = [c for c in calls if ":CALLS" in c.args[0]]

        assert len(node_calls) == 1
        assert len(node_calls[0].kwargs["batch"]) >= 1

        assert len(edge_calls) == 1
        assert len(edge_calls[0].kwargs["batch"]) == 3

    @pytest.mark.asyncio
    async def test_regular_edges_still_batched(self) -> None:
        driver, _, tx = _mock_driver()
        hot_config = HotTargetConfig(
            hot_target_threshold=100, hot_target_max_concurrent=1,
        )
        repo = GraphRepository(driver, hot_target_config=hot_config)

        edges = [
            CallsEdge(
                source_service_id=f"svc-{i}",
                target_service_id=f"target-{i}",
                protocol="http",
                tenant_id="test-tenant",
            )
            for i in range(5)
        ]
        node = ServiceNode(
            id="svc-0", name="svc-0", language="go",
            framework="gin", opentelemetry_enabled=False,
            tenant_id="test-tenant",
        )

        await repo.commit_topology([node] + edges)

        edge_calls = [
            c for c in tx.run.call_args_list if ":CALLS" in c.args[0]
        ]
        assert len(edge_calls) == 1
        assert len(edge_calls[0].kwargs["batch"]) == 5

    @pytest.mark.asyncio
    async def test_mixed_batch_splits_correctly(self) -> None:
        driver, _, tx = _mock_driver()
        hot_config = HotTargetConfig(
            hot_target_threshold=3, hot_target_max_concurrent=1,
        )
        repo = GraphRepository(driver, hot_target_config=hot_config)

        hot_edges = [
            CallsEdge(
                source_service_id=f"caller-{i}",
                target_service_id="central-auth",
                protocol="http",
                tenant_id="test-tenant",
            )
            for i in range(4)
        ]
        cold_edges = [
            CallsEdge(
                source_service_id=f"cold-src-{i}",
                target_service_id=f"cold-tgt-{i}",
                protocol="grpc",
                tenant_id="test-tenant",
            )
            for i in range(3)
        ]

        await repo.commit_topology(hot_edges + cold_edges)

        edge_calls = [
            c for c in tx.run.call_args_list if ":CALLS" in c.args[0]
        ]
        batch_sizes = sorted(
            [len(c.kwargs["batch"]) for c in edge_calls],
        )
        assert sum(batch_sizes) == 7
        assert all(s >= 1 for s in batch_sizes)

    @pytest.mark.asyncio
    async def test_default_config_batches_below_threshold(self) -> None:
        driver, _, tx = _mock_driver()
        repo = GraphRepository(driver)

        edges = [
            CallsEdge(
                source_service_id=f"svc-{i}",
                target_service_id="shared-hub",
                protocol="http",
                tenant_id="test-tenant",
            )
            for i in range(9)
        ]

        await repo.commit_topology(edges)

        edge_calls = [
            c for c in tx.run.call_args_list if ":CALLS" in c.args[0]
        ]
        assert len(edge_calls) == 1
        assert len(edge_calls[0].kwargs["batch"]) == 9
