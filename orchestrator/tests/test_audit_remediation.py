from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from orchestrator.app.distributed_lock import DistributedLock
from orchestrator.app.vector_sync_outbox import VectorSyncEvent


class TestLockHeartbeatRenewal:

    @pytest.mark.asyncio
    async def test_heartbeat_extends_ttl_while_held(self) -> None:
        redis_mock = AsyncMock()
        redis_mock.set = AsyncMock(return_value=True)
        redis_mock.eval = AsyncMock(return_value=1)

        lock = DistributedLock(redis_conn=redis_mock, key_prefix="test:")
        async with lock.acquire("mykey", ttl=3, heartbeat_interval=0.05):
            await asyncio.sleep(0.15)

        renew_calls = [
            c for c in redis_mock.eval.call_args_list
            if len(c[0]) >= 1 and "pexpire" in str(c[0][0]).lower()
        ]
        assert len(renew_calls) >= 2

    @pytest.mark.asyncio
    async def test_heartbeat_stops_after_release(self) -> None:
        redis_mock = AsyncMock()
        redis_mock.set = AsyncMock(return_value=True)
        redis_mock.eval = AsyncMock(return_value=1)

        lock = DistributedLock(redis_conn=redis_mock, key_prefix="test:")
        async with lock.acquire("mykey", ttl=3, heartbeat_interval=0.05):
            pass

        redis_mock.eval.reset_mock()
        await asyncio.sleep(0.15)

        renew_calls = [
            c for c in redis_mock.eval.call_args_list
            if len(c[0]) >= 1 and "pexpire" in str(c[0][0]).lower()
        ]
        assert len(renew_calls) == 0

    @pytest.mark.asyncio
    async def test_heartbeat_no_extend_if_owner_changed(self) -> None:
        redis_mock = AsyncMock()
        redis_mock.set = AsyncMock(return_value=True)
        redis_mock.eval = AsyncMock(return_value=0)

        lock = DistributedLock(redis_conn=redis_mock, key_prefix="test:")
        async with lock.acquire("mykey", ttl=3, heartbeat_interval=0.05):
            await asyncio.sleep(0.15)

    @pytest.mark.asyncio
    async def test_heartbeat_cancelled_on_exception(self) -> None:
        redis_mock = AsyncMock()
        redis_mock.set = AsyncMock(return_value=True)
        redis_mock.eval = AsyncMock(return_value=1)

        lock = DistributedLock(redis_conn=redis_mock, key_prefix="test:")
        with pytest.raises(ValueError, match="boom"):
            async with lock.acquire("mykey", ttl=3, heartbeat_interval=0.05):
                raise ValueError("boom")

        redis_mock.eval.reset_mock()
        await asyncio.sleep(0.15)
        renew_calls = [
            c for c in redis_mock.eval.call_args_list
            if len(c[0]) >= 1 and "pexpire" in str(c[0][0]).lower()
        ]
        assert len(renew_calls) == 0


class TestOutboxProductionGuard:

    @pytest.mark.asyncio
    async def test_production_rejects_inmemory_enqueue(self) -> None:
        from orchestrator.app.graph_builder import _enqueue_vector_cleanup

        span_mock = MagicMock()
        span_mock.set_attribute = MagicMock()

        with patch.dict(
            "os.environ",
            {"DEPLOYMENT_MODE": "production", "OUTBOX_COALESCING": "true"},
            clear=False,
        ):
            with pytest.raises(SystemExit):
                await _enqueue_vector_cleanup(
                    pruned_ids=["node-1"],
                    span=span_mock,
                    tenant_id="t1",
                    neo4j_driver=None,
                )

    @pytest.mark.asyncio
    async def test_dev_allows_inmemory_enqueue(self) -> None:
        from orchestrator.app.graph_builder import _enqueue_vector_cleanup

        span_mock = MagicMock()
        span_mock.set_attribute = MagicMock()

        with patch.dict(
            "os.environ",
            {"DEPLOYMENT_MODE": "development", "OUTBOX_COALESCING": "true"},
            clear=False,
        ):
            await _enqueue_vector_cleanup(
                pruned_ids=["node-1"],
                span=span_mock,
                tenant_id="t1",
                neo4j_driver=None,
            )

    @pytest.mark.asyncio
    async def test_durable_driver_bypasses_guard(self) -> None:
        from orchestrator.app.graph_builder import _enqueue_vector_cleanup

        span_mock = MagicMock()
        span_mock.set_attribute = MagicMock()
        mock_driver = MagicMock()

        with (
            patch.dict(
                "os.environ",
                {"DEPLOYMENT_MODE": "production"},
                clear=False,
            ),
            patch(
                "orchestrator.app.graph_builder.Neo4jOutboxStore",
            ) as mock_store_cls,
        ):
            mock_store = AsyncMock()
            mock_store_cls.return_value = mock_store
            await _enqueue_vector_cleanup(
                pruned_ids=["node-1"],
                span=span_mock,
                tenant_id="t1",
                neo4j_driver=mock_driver,
            )
            mock_store.write_event.assert_awaited_once()


class TestBatchedDegreeCheck:

    @pytest.mark.asyncio
    async def test_batch_returns_degree_map(self) -> None:
        from orchestrator.app.agentic_traversal import batch_check_degrees

        mock_driver = MagicMock()
        mock_session = AsyncMock()
        mock_driver.session.return_value.__aenter__ = AsyncMock(
            return_value=mock_session,
        )
        mock_driver.session.return_value.__aexit__ = AsyncMock(
            return_value=False,
        )
        mock_session.execute_read = AsyncMock(return_value=[
            {"node_id": "svc-a", "degree": 300},
            {"node_id": "svc-b", "degree": 10},
        ])

        result = await batch_check_degrees(
            driver=mock_driver,
            source_ids=["svc-a", "svc-b"],
            tenant_id="t1",
        )
        assert result == {"svc-a": 300, "svc-b": 10}

    @pytest.mark.asyncio
    async def test_empty_ids_returns_empty_map(self) -> None:
        from orchestrator.app.agentic_traversal import batch_check_degrees

        mock_driver = MagicMock()
        result = await batch_check_degrees(
            driver=mock_driver,
            source_ids=[],
            tenant_id="t1",
        )
        assert result == {}

    @pytest.mark.asyncio
    async def test_batch_uses_single_query(self) -> None:
        from orchestrator.app.agentic_traversal import batch_check_degrees

        mock_driver = MagicMock()
        mock_session = AsyncMock()
        mock_driver.session.return_value.__aenter__ = AsyncMock(
            return_value=mock_session,
        )
        mock_driver.session.return_value.__aexit__ = AsyncMock(
            return_value=False,
        )
        mock_session.execute_read = AsyncMock(return_value=[
            {"node_id": "svc-a", "degree": 5},
            {"node_id": "svc-b", "degree": 15},
            {"node_id": "svc-c", "degree": 25},
        ])

        await batch_check_degrees(
            driver=mock_driver,
            source_ids=["svc-a", "svc-b", "svc-c"],
            tenant_id="t1",
        )
        mock_session.execute_read.assert_awaited_once()


class TestSanitizationInputCap:

    def test_oversized_source_raises(self) -> None:
        from orchestrator.app.prompt_sanitizer import (
            SanitizationBudgetExceeded,
            sanitize_source_content,
        )

        huge_input = "A" * 200_000
        with pytest.raises(SanitizationBudgetExceeded):
            sanitize_source_content(
                huge_input, "file.py", max_chars=1_000_000,
                max_input_bytes=100_000,
            )

    def test_normal_input_passes(self) -> None:
        from orchestrator.app.prompt_sanitizer import sanitize_source_content

        result = sanitize_source_content(
            "normal content", "file.py",
            max_input_bytes=100_000,
        )
        assert "normal content" in result

    def test_firewall_scan_rejects_oversized(self) -> None:
        from orchestrator.app.prompt_sanitizer import (
            ContentFirewall,
            SanitizationBudgetExceeded,
        )

        fw = ContentFirewall(max_input_bytes=1000)
        huge = "x" * 2000
        with pytest.raises(SanitizationBudgetExceeded):
            fw.scan(huge)

    def test_classifier_rejects_oversized(self) -> None:
        from orchestrator.app.prompt_sanitizer import (
            PromptInjectionClassifier,
            SanitizationBudgetExceeded,
        )

        cls = PromptInjectionClassifier(max_input_bytes=1000)
        huge = "y" * 2000
        with pytest.raises(SanitizationBudgetExceeded):
            cls.classify(huge)


class TestCacheInvalidationGuard:

    @pytest.mark.asyncio
    async def test_tenant_fallback_logs_warning(self) -> None:
        from orchestrator.app.graph_builder import invalidate_caches_after_ingest

        mock_subgraph_cache = MagicMock()
        mock_subgraph_cache.invalidate_tenant = MagicMock(return_value=None)
        mock_semantic_cache = MagicMock()
        mock_semantic_cache.invalidate_tenant = MagicMock(return_value=None)

        with (
            patch(
                "orchestrator.app.graph_builder._SUBGRAPH_CACHE",
                mock_subgraph_cache,
                create=True,
            ),
            patch(
                "orchestrator.app.graph_builder._SEMANTIC_CACHE",
                mock_semantic_cache,
                create=True,
            ),
            patch("orchestrator.app.graph_builder.logger") as mock_logger,
        ):
            await invalidate_caches_after_ingest(
                tenant_id="t1", node_ids=None,
            )
            warning_calls = [
                c for c in mock_logger.warning.call_args_list
                if "tenant-wide" in str(c).lower()
                or "fallback" in str(c).lower()
            ]
            assert len(warning_calls) >= 1

    @pytest.mark.asyncio
    async def test_node_level_does_not_warn(self) -> None:
        from orchestrator.app.graph_builder import invalidate_caches_after_ingest

        mock_subgraph_cache = MagicMock()
        mock_subgraph_cache.invalidate_by_nodes = MagicMock(return_value=None)
        mock_semantic_cache = MagicMock()
        mock_semantic_cache.invalidate_by_nodes = MagicMock(return_value=None)

        with (
            patch(
                "orchestrator.app.graph_builder._SUBGRAPH_CACHE",
                mock_subgraph_cache,
                create=True,
            ),
            patch(
                "orchestrator.app.graph_builder._SEMANTIC_CACHE",
                mock_semantic_cache,
                create=True,
            ),
            patch("orchestrator.app.graph_builder.logger") as mock_logger,
        ):
            await invalidate_caches_after_ingest(
                tenant_id="t1", node_ids={"node-1", "node-2"},
            )
            warning_calls = [
                c for c in mock_logger.warning.call_args_list
                if "tenant-wide" in str(c).lower()
                or "fallback" in str(c).lower()
            ]
            assert len(warning_calls) == 0
