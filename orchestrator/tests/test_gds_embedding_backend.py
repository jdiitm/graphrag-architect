from __future__ import annotations

from typing import Dict, List, runtime_checkable
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.graph_embeddings import (
    GraphEmbeddingBackend,
    GraphTooLargeError,
    GraphTopology,
    GDSEmbeddingBackend,
    LocalEmbeddingBackend,
    Node2VecConfig,
    create_embedding_backend,
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


class TestGraphEmbeddingBackendProtocol:
    def test_protocol_is_runtime_checkable(self) -> None:
        assert runtime_checkable(GraphEmbeddingBackend)

    def test_local_backend_implements_protocol(self) -> None:
        backend = LocalEmbeddingBackend()
        assert isinstance(backend, GraphEmbeddingBackend)

    def test_gds_backend_implements_protocol(self) -> None:
        driver = MagicMock()
        backend = GDSEmbeddingBackend(driver=driver)
        assert isinstance(backend, GraphEmbeddingBackend)


class TestLocalEmbeddingBackend:
    def test_embed_produces_embeddings_for_all_nodes(self) -> None:
        topo = _small_topology()
        config = Node2VecConfig(
            num_walks=2, walk_length=5, embedding_dim=32, seed=42,
        )
        backend = LocalEmbeddingBackend(config=config)
        embeddings = backend.generate_embeddings(topo)
        assert set(embeddings.keys()) == {"a", "b", "c"}
        for emb in embeddings.values():
            assert len(emb) == 32

    def test_raises_graph_too_large_for_exceeding_limit(self) -> None:
        topo = _large_topology(5001)
        backend = LocalEmbeddingBackend()
        with pytest.raises(GraphTooLargeError):
            backend.generate_embeddings(topo)

    def test_works_for_graphs_under_limit(self) -> None:
        topo = _small_topology()
        backend = LocalEmbeddingBackend()
        embeddings = backend.generate_embeddings(topo)
        assert len(embeddings) == 3


class TestGDSEmbeddingBackend:
    @pytest.mark.asyncio
    async def test_generates_embeddings_via_cypher_procedure(self) -> None:
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

        backend = GDSEmbeddingBackend(driver=mock_driver)
        topo = GraphTopology(
            nodes=["a", "b"],
            adjacency={"a": ["b"], "b": ["a"]},
        )
        embeddings = await backend.generate_embeddings_async(topo)

        assert set(embeddings.keys()) == {"a", "b"}
        assert len(embeddings["a"]) == 128
        assert len(embeddings["b"]) == 128

    @pytest.mark.asyncio
    async def test_no_node_count_limit(self) -> None:
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

        backend = GDSEmbeddingBackend(driver=mock_driver)
        topo = _large_topology(6000)

        embeddings = await backend.generate_embeddings_async(topo)
        assert len(embeddings) == 6000

    @pytest.mark.asyncio
    async def test_calls_gds_node2vec_stream_with_correct_params(self) -> None:
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

        config = Node2VecConfig(
            walk_length=80,
            num_walks=10,
            embedding_dim=128,
            p=1.0,
            q=0.5,
        )
        backend = GDSEmbeddingBackend(driver=mock_driver, config=config)
        topo = _small_topology()

        await backend.generate_embeddings_async(topo)

        call_args = mock_session.run.call_args
        cypher_query = call_args[0][0]
        assert "gds.node2vec.stream" in cypher_query

        params = call_args[1] if len(call_args) > 1 else call_args[0][1] if len(call_args[0]) > 1 else {}
        if isinstance(params, dict):
            assert params.get("embeddingDimension") == 128
            assert params.get("walkLength") == 80
            assert params.get("walksPerNode") == 10
            assert params.get("returnFactor") == 1.0
            assert params.get("inOutFactor") == 0.5

    @pytest.mark.asyncio
    async def test_returns_dict_of_float_vectors(self) -> None:
        mock_record = MagicMock()
        mock_record.__getitem__ = lambda self, key: {
            "nodeId": "x", "embedding": [0.5, 0.3, 0.1],
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

        backend = GDSEmbeddingBackend(driver=mock_driver)
        topo = GraphTopology(nodes=["x"], adjacency={})
        embeddings = await backend.generate_embeddings_async(topo)

        assert isinstance(embeddings, dict)
        assert isinstance(embeddings["x"], list)
        assert all(isinstance(v, float) for v in embeddings["x"])

    @pytest.mark.asyncio
    async def test_handles_gds_plugin_not_installed(self) -> None:
        from neo4j.exceptions import ClientError

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(
            side_effect=ClientError(
                "There is no procedure with the name `gds.node2vec.stream`",
            ),
        )
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = AsyncMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        backend = GDSEmbeddingBackend(driver=mock_driver)
        topo = _small_topology()

        with pytest.raises(RuntimeError, match="Neo4j GDS plugin"):
            await backend.generate_embeddings_async(topo)

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

        backend = GDSEmbeddingBackend(driver=mock_driver)
        topo = GraphTopology(nodes=["a"], adjacency={})
        embeddings = backend.generate_embeddings(topo)

        assert isinstance(embeddings, dict)
        assert "a" in embeddings


class TestBackendFactory:
    def test_local_backend_from_config(self) -> None:
        backend = create_embedding_backend("local")
        assert isinstance(backend, LocalEmbeddingBackend)

    def test_gds_backend_from_config(self) -> None:
        mock_driver = MagicMock()
        backend = create_embedding_backend("gds", driver=mock_driver)
        assert isinstance(backend, GDSEmbeddingBackend)

    def test_default_is_local(self) -> None:
        backend = create_embedding_backend()
        assert isinstance(backend, LocalEmbeddingBackend)

    def test_invalid_backend_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Unknown.*backend"):
            create_embedding_backend("invalid")

    @patch.dict("os.environ", {"GRAPH_EMBEDDING_BACKEND": "gds"})
    def test_env_var_selects_gds(self) -> None:
        import os
        assert os.environ["GRAPH_EMBEDDING_BACKEND"] == "gds"

    @patch.dict("os.environ", {"GRAPH_EMBEDDING_BACKEND": "local"})
    def test_env_var_selects_local(self) -> None:
        import os
        assert os.environ["GRAPH_EMBEDDING_BACKEND"] == "local"
