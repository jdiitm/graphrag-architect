from __future__ import annotations

import os
from typing import Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.graph_embeddings import (
    GraphEmbeddingBackend,
    GraphTopology,
    GDSEmbeddingBackend,
    LocalEmbeddingBackend,
    create_embedding_backend,
    FastRPConfig,
    FastRPEmbeddingBackend,
)


def _small_topology() -> GraphTopology:
    return GraphTopology(
        nodes=["a", "b", "c"],
        adjacency={"a": ["b"], "b": ["a", "c"], "c": ["b"]},
    )


def _large_topology(n: int) -> GraphTopology:
    nodes = [f"node_{i}" for i in range(n)]
    adjacency: Dict[str, List[str]] = {}
    for i, node in enumerate(nodes):
        neighbors = []
        if i > 0:
            neighbors.append(nodes[i - 1])
        if i < n - 1:
            neighbors.append(nodes[i + 1])
        adjacency[node] = neighbors
    return GraphTopology(nodes=nodes, adjacency=adjacency)


class TestFastRPEmbeddingBackendProtocol:
    def test_implements_graph_embedding_backend(self) -> None:
        driver = MagicMock()
        backend = FastRPEmbeddingBackend(driver=driver)
        assert isinstance(backend, GraphEmbeddingBackend)


class TestFastRPEmbeddingBackendAsync:
    @pytest.mark.asyncio
    async def test_calls_gds_fastrp_stream_with_correct_params(self) -> None:
        mock_result = AsyncMock()
        mock_result.__aiter__ = lambda self: self
        mock_result._records = []
        mock_result.__anext__ = AsyncMock(side_effect=[StopAsyncIteration()])

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = AsyncMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        config = FastRPConfig(
            embedding_dim=64,
            iteration_weights=(0.0, 1.0, 1.0),
            normalization_strength=0.5,
        )
        backend = FastRPEmbeddingBackend(
            driver=mock_driver, config=config, graph_name="test_graph",
        )
        topo = _small_topology()

        await backend.generate_embeddings_async(topo)

        call_args = mock_session.run.call_args
        cypher_query = call_args[0][0]
        assert "gds.fastRP.stream" in cypher_query

        params = call_args[1]
        assert params["graphName"] == "test_graph"
        assert params["embeddingDimension"] == 64
        assert params["iterationWeights"] == [0.0, 1.0, 1.0]
        assert params["normalizationStrength"] == 0.5

    @pytest.mark.asyncio
    async def test_no_node_count_limit_6000_nodes(self) -> None:
        records = []
        for i in range(6000):
            rec = MagicMock()
            rec.__getitem__ = (
                lambda self, key, _i=i: {
                    "nodeId": f"node_{_i}",
                    "embedding": [0.0] * 128,
                }[key]
            )
            records.append(rec)

        mock_result = AsyncMock()
        mock_result.__aiter__ = lambda self: self
        mock_result._records = list(records)
        mock_result.__anext__ = AsyncMock(
            side_effect=records + [StopAsyncIteration()],
        )

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = AsyncMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        backend = FastRPEmbeddingBackend(driver=mock_driver)
        topo = _large_topology(6000)

        embeddings = await backend.generate_embeddings_async(topo)
        assert len(embeddings) == 6000

    @pytest.mark.asyncio
    async def test_handles_gds_plugin_not_installed(self) -> None:
        from neo4j.exceptions import ClientError

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(
            side_effect=ClientError(
                "There is no procedure with the name `gds.fastRP.stream`",
            ),
        )
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = AsyncMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        backend = FastRPEmbeddingBackend(driver=mock_driver)
        topo = _small_topology()

        with pytest.raises(RuntimeError, match="Neo4j GDS plugin"):
            await backend.generate_embeddings_async(topo)

    @pytest.mark.asyncio
    async def test_generates_embeddings_for_nodes(self) -> None:
        mock_record_a = MagicMock()
        mock_record_a.__getitem__ = lambda self, key: {
            "nodeId": "a", "embedding": [0.1] * 128,
        }[key]
        mock_record_b = MagicMock()
        mock_record_b.__getitem__ = lambda self, key: {
            "nodeId": "b", "embedding": [0.2] * 128,
        }[key]

        mock_result = AsyncMock()
        mock_result.__aiter__ = lambda self: self
        mock_result_records = [mock_record_a, mock_record_b]
        mock_result._records = list(mock_result_records)
        mock_result.__anext__ = AsyncMock(
            side_effect=mock_result_records + [StopAsyncIteration()],
        )

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = AsyncMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        backend = FastRPEmbeddingBackend(driver=mock_driver)
        topo = GraphTopology(
            nodes=["a", "b"],
            adjacency={"a": ["b"], "b": ["a"]},
        )
        embeddings = await backend.generate_embeddings_async(topo)

        assert set(embeddings.keys()) == {"a", "b"}
        assert len(embeddings["a"]) == 128
        assert all(isinstance(v, float) for v in embeddings["a"])


class TestFastRPSyncWrapper:
    def test_sync_generate_embeddings_delegates_to_async(self) -> None:
        mock_record = MagicMock()
        mock_record.__getitem__ = lambda self, key: {
            "nodeId": "a", "embedding": [0.1] * 128,
        }[key]

        mock_result = AsyncMock()
        mock_result.__aiter__ = lambda self: self
        mock_result._records = [mock_record]
        mock_result.__anext__ = AsyncMock(
            side_effect=[mock_record, StopAsyncIteration()],
        )

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = AsyncMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        backend = FastRPEmbeddingBackend(driver=mock_driver)
        topo = GraphTopology(nodes=["a"], adjacency={})
        embeddings = backend.generate_embeddings(topo)

        assert isinstance(embeddings, dict)
        assert "a" in embeddings
        assert len(embeddings["a"]) == 128


class TestFastRPBackendFactory:
    def test_fastrp_backend_from_factory(self) -> None:
        mock_driver = MagicMock()
        backend = create_embedding_backend("fastrp", driver=mock_driver)
        assert isinstance(backend, FastRPEmbeddingBackend)

    def test_local_backward_compat(self) -> None:
        backend = create_embedding_backend("local")
        assert isinstance(backend, LocalEmbeddingBackend)

    def test_gds_backward_compat(self) -> None:
        mock_driver = MagicMock()
        backend = create_embedding_backend("gds", driver=mock_driver)
        assert isinstance(backend, GDSEmbeddingBackend)


class TestFastRPConfig:
    def test_defaults_are_sensible(self) -> None:
        config = FastRPConfig()
        assert config.embedding_dim == 128
        assert config.iteration_weights == (0.0, 1.0, 1.0, 0.8)
        assert config.normalization_strength == 0.0

    @patch.dict(os.environ, {
        "FASTRP_EMBEDDING_DIM": "256",
        "FASTRP_ITERATION_WEIGHTS": "0.0,1.0,2.0",
        "FASTRP_NORMALIZATION_STRENGTH": "0.5",
    })
    def test_from_env_reads_env_vars(self) -> None:
        config = FastRPConfig.from_env()
        assert config.embedding_dim == 256
        assert config.iteration_weights == (0.0, 1.0, 2.0)
        assert config.normalization_strength == 0.5

    @patch.dict(os.environ, {}, clear=True)
    def test_from_env_uses_defaults_when_unset(self) -> None:
        config = FastRPConfig.from_env()
        assert config.embedding_dim == 128
        assert config.iteration_weights == (0.0, 1.0, 1.0, 0.8)
        assert config.normalization_strength == 0.0

    def test_config_is_frozen(self) -> None:
        config = FastRPConfig()
        with pytest.raises(AttributeError):
            config.embedding_dim = 256  # type: ignore[misc]
