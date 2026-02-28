from __future__ import annotations

import asyncio
from typing import List
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.embedding_batcher import (
    EmbeddingBatcher,
    EmbeddingBatcherConfig,
    RateLimitError,
)


def _make_provider(batch_results: List[List[List[float]]] | None = None) -> AsyncMock:
    provider = AsyncMock()
    if batch_results is not None:
        provider.embed_batch.side_effect = list(batch_results)
    else:
        async def _dynamic_embed(texts: List[str]) -> List[List[float]]:
            return [[0.1, 0.2, 0.3] for _ in texts]
        provider.embed_batch.side_effect = _dynamic_embed
    return provider


class TestBatchAccumulation:
    @pytest.mark.asyncio
    async def test_100_items_with_batch_size_50_produces_2_flushes(self) -> None:
        provider = _make_provider()
        config = EmbeddingBatcherConfig(max_batch_size=50, flush_interval_ms=5000)
        async with EmbeddingBatcher(provider, config) as batcher:
            futures = [
                batcher.submit(f"text-{i}", {"idx": i}) for i in range(100)
            ]
            results = await asyncio.gather(*futures)

        assert provider.embed_batch.call_count == 2
        for result in results:
            assert result == [0.1, 0.2, 0.3]

    @pytest.mark.asyncio
    async def test_batch_never_exceeds_max_batch_size(self) -> None:
        provider = _make_provider()
        config = EmbeddingBatcherConfig(max_batch_size=25, flush_interval_ms=5000)
        async with EmbeddingBatcher(provider, config) as batcher:
            futures = [
                batcher.submit(f"text-{i}", {"idx": i}) for i in range(100)
            ]
            await asyncio.gather(*futures)

        assert provider.embed_batch.call_count == 4
        for call in provider.embed_batch.call_args_list:
            texts = call.args[0] if call.args else call.kwargs.get("texts", [])
            assert len(texts) <= 25


class TestPartialFlush:
    @pytest.mark.asyncio
    async def test_partial_batch_flushed_on_timer(self) -> None:
        provider = _make_provider()
        config = EmbeddingBatcherConfig(max_batch_size=50, flush_interval_ms=50)
        async with EmbeddingBatcher(provider, config) as batcher:
            futures = [
                batcher.submit(f"text-{i}", {"idx": i}) for i in range(30)
            ]
            results = await asyncio.gather(*futures)

        assert provider.embed_batch.call_count >= 1
        total_texts = sum(
            len(call.args[0] if call.args else call.kwargs.get("texts", []))
            for call in provider.embed_batch.call_args_list
        )
        assert total_texts == 30
        assert all(r == [0.1, 0.2, 0.3] for r in results)


class TestRateLimitBackoff:
    @pytest.mark.asyncio
    async def test_rate_limit_triggers_exponential_backoff_then_succeeds(
        self,
    ) -> None:
        provider = AsyncMock()
        provider.embed_batch.side_effect = [
            RateLimitError("rate limited"),
            RateLimitError("rate limited"),
            [[0.5, 0.6, 0.7] for _ in range(10)],
        ]
        config = EmbeddingBatcherConfig(
            max_batch_size=10, flush_interval_ms=50, max_retries=5,
        )
        async with EmbeddingBatcher(provider, config) as batcher:
            futures = [
                batcher.submit(f"text-{i}", {"idx": i}) for i in range(10)
            ]
            results = await asyncio.gather(*futures)

        assert provider.embed_batch.call_count == 3
        assert all(r == [0.5, 0.6, 0.7] for r in results)

    @pytest.mark.asyncio
    async def test_permanent_failure_after_retries_propagates_error(self) -> None:
        provider = AsyncMock()
        provider.embed_batch.side_effect = RateLimitError("always rate limited")
        config = EmbeddingBatcherConfig(
            max_batch_size=10, flush_interval_ms=50, max_retries=3,
        )

        async with EmbeddingBatcher(provider, config) as batcher:
            futures = [
                batcher.submit(f"text-{i}", {"idx": i}) for i in range(10)
            ]
            with pytest.raises(RateLimitError):
                await asyncio.gather(*futures)

        assert provider.embed_batch.call_count == 4


class TestGracefulShutdown:
    @pytest.mark.asyncio
    async def test_close_flushes_remaining_items(self) -> None:
        provider = _make_provider()
        config = EmbeddingBatcherConfig(max_batch_size=100, flush_interval_ms=60000)
        batcher = EmbeddingBatcher(provider, config)
        await batcher.start()
        futures = [
            batcher.submit(f"text-{i}", {"idx": i}) for i in range(7)
        ]
        await batcher.close()
        results = await asyncio.gather(*futures)

        assert provider.embed_batch.call_count == 1
        texts_sent = provider.embed_batch.call_args.args[0]
        assert len(texts_sent) == 7
        assert all(r == [0.1, 0.2, 0.3] for r in results)

    @pytest.mark.asyncio
    async def test_close_on_empty_batcher_does_not_call_provider(self) -> None:
        provider = _make_provider()
        config = EmbeddingBatcherConfig(max_batch_size=100, flush_interval_ms=60000)
        batcher = EmbeddingBatcher(provider, config)
        await batcher.start()
        await batcher.close()

        provider.embed_batch.assert_not_called()


class TestConcurrentSubmissions:
    @pytest.mark.asyncio
    async def test_concurrent_coroutines_all_items_flushed(self) -> None:
        provider = _make_provider()
        config = EmbeddingBatcherConfig(max_batch_size=20, flush_interval_ms=50)

        async def submit_batch(
            batcher: EmbeddingBatcher, start: int, count: int,
        ) -> List[List[float]]:
            futs = [
                batcher.submit(f"text-{start + i}", {"idx": start + i})
                for i in range(count)
            ]
            return await asyncio.gather(*futs)

        async with EmbeddingBatcher(provider, config) as batcher:
            tasks = [
                submit_batch(batcher, i * 20, 20) for i in range(5)
            ]
            all_results = await asyncio.gather(*tasks)

        total_embedded = sum(len(batch) for batch in all_results)
        assert total_embedded == 100

        total_texts_sent = sum(
            len(call.args[0] if call.args else call.kwargs.get("texts", []))
            for call in provider.embed_batch.call_args_list
        )
        assert total_texts_sent == 100


class TestNonRateLimitErrors:
    @pytest.mark.asyncio
    async def test_non_rate_limit_error_propagates_to_futures(self) -> None:
        provider = AsyncMock()
        provider.embed_batch.side_effect = ConnectionError("network blip")
        config = EmbeddingBatcherConfig(
            max_batch_size=10, flush_interval_ms=50, max_retries=0,
        )
        async with EmbeddingBatcher(provider, config) as batcher:
            futures = [
                batcher.submit(f"text-{i}", {"idx": i}) for i in range(5)
            ]
            with pytest.raises(ConnectionError):
                await asyncio.wait_for(asyncio.gather(*futures), timeout=2.0)

    @pytest.mark.asyncio
    async def test_batcher_recovers_after_non_rate_limit_error(self) -> None:
        call_count = 0

        async def _embed(texts: List[str]) -> List[List[float]]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("network blip")
            return [[0.1, 0.2, 0.3] for _ in texts]

        provider = AsyncMock()
        provider.embed_batch.side_effect = _embed
        config = EmbeddingBatcherConfig(
            max_batch_size=5, flush_interval_ms=50,
        )
        async with EmbeddingBatcher(provider, config) as batcher:
            first_futures = [
                batcher.submit(f"a-{i}", {"idx": i}) for i in range(5)
            ]
            with pytest.raises(ConnectionError):
                await asyncio.wait_for(
                    asyncio.gather(*first_futures), timeout=2.0,
                )

            second_futures = [
                batcher.submit(f"b-{i}", {"idx": i}) for i in range(5)
            ]
            results = await asyncio.wait_for(
                asyncio.gather(*second_futures), timeout=2.0,
            )

        assert all(r == [0.1, 0.2, 0.3] for r in results)


class TestCloseWithInflightBatch:
    @pytest.mark.asyncio
    async def test_close_during_send_completes_inflight_batch(self) -> None:
        send_started = asyncio.Event()
        send_proceed = asyncio.Event()

        async def _slow_embed(texts: List[str]) -> List[List[float]]:
            send_started.set()
            await send_proceed.wait()
            return [[0.1, 0.2, 0.3] for _ in texts]

        provider = AsyncMock()
        provider.embed_batch.side_effect = _slow_embed
        config = EmbeddingBatcherConfig(
            max_batch_size=100, flush_interval_ms=50,
        )

        batcher = EmbeddingBatcher(provider, config)
        await batcher.start()

        futures = [
            batcher.submit(f"text-{i}", {"idx": i}) for i in range(5)
        ]

        await send_started.wait()

        close_task = asyncio.create_task(batcher.close())
        await asyncio.sleep(0.01)
        send_proceed.set()

        await asyncio.wait_for(close_task, timeout=2.0)

        results = await asyncio.wait_for(
            asyncio.gather(*futures), timeout=2.0,
        )
        assert all(r == [0.1, 0.2, 0.3] for r in results)


class TestEmbedBatchLengthValidation:
    @pytest.mark.asyncio
    async def test_provider_returns_fewer_embeddings_raises_error(self) -> None:
        provider = AsyncMock()
        provider.embed_batch.return_value = [[0.1, 0.2]]
        config = EmbeddingBatcherConfig(
            max_batch_size=10, flush_interval_ms=50,
        )
        async with EmbeddingBatcher(provider, config) as batcher:
            futures = [
                batcher.submit(f"text-{i}", {"idx": i}) for i in range(5)
            ]
            with pytest.raises(
                ValueError, match="returned 1 embeddings for 5 texts",
            ):
                await asyncio.wait_for(asyncio.gather(*futures), timeout=2.0)


class TestConfigDefaults:
    def test_frozen_config_defaults(self) -> None:
        config = EmbeddingBatcherConfig()
        assert config.max_batch_size == 2048
        assert config.flush_interval_ms == 500
        assert config.max_retries == 3

    def test_frozen_config_immutable(self) -> None:
        config = EmbeddingBatcherConfig()
        with pytest.raises(AttributeError):
            config.max_batch_size = 99  # type: ignore[misc]
