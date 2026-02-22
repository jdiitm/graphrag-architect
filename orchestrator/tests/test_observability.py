from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

from orchestrator.app.observability import (
    ErrorForceExportProcessor,
    RecordAllSampler,
    _build_sampler,
    configure_telemetry,
    get_tracer,
)


class MemoryExporter(SpanExporter):
    def __init__(self):
        self.spans = []

    def export(self, spans):
        self.spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self):
        pass

    def get_finished_spans(self):
        return list(self.spans)

    def clear(self):
        self.spans.clear()


@pytest.fixture
def telemetry():
    exporter = MemoryExporter()
    provider = configure_telemetry(exporter=exporter)
    yield exporter
    provider.shutdown()


class TestConfigureTelemetry:
    def test_returns_tracer_provider(self):
        exporter = MemoryExporter()
        provider = configure_telemetry(exporter=exporter)
        assert isinstance(provider, TracerProvider)
        provider.shutdown()

    def test_get_tracer_returns_named_tracer(self):
        exporter = MemoryExporter()
        provider = configure_telemetry(exporter=exporter)
        tracer = get_tracer()
        assert tracer is not None
        provider.shutdown()


class TestIngestionSpans:
    def test_load_workspace_creates_span(self, telemetry):
        from orchestrator.app.graph_builder import load_workspace_files

        state = {
            "directory_path": "",
            "raw_files": [{"path": "a.go", "content": "package main"}],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        load_workspace_files(state)
        span_names = [s.name for s in telemetry.get_finished_spans()]
        assert "ingestion.load_workspace" in span_names

    @pytest.mark.asyncio
    async def test_parse_services_creates_span(self, telemetry):
        from orchestrator.app.graph_builder import parse_go_and_python_services

        state = {
            "directory_path": "",
            "raw_files": [],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        with patch(
            "orchestrator.app.graph_builder.ServiceExtractor"
        ) as mock_cls, patch.dict(
            "os.environ", {"GOOGLE_API_KEY": "test-key"}
        ):
            mock_extractor = MagicMock()
            mock_extractor.extract_all = AsyncMock(
                return_value=MagicMock(services=[], calls=[])
            )
            mock_cls.return_value = mock_extractor
            await parse_go_and_python_services(state)

        span_names = [s.name for s in telemetry.get_finished_spans()]
        assert "ingestion.parse_services" in span_names

    def test_validate_schema_creates_span(self, telemetry):
        from orchestrator.app.graph_builder import validate_extracted_schema

        state = {
            "directory_path": "",
            "raw_files": [],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        validate_extracted_schema(state)
        span_names = [s.name for s in telemetry.get_finished_spans()]
        assert "ingestion.validate_schema" in span_names

    @pytest.mark.asyncio
    async def test_commit_neo4j_creates_span(self, telemetry):
        from orchestrator.app.graph_builder import commit_to_neo4j

        state = {
            "directory_path": "",
            "raw_files": [],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.execute_write = AsyncMock()
        mock_driver.session.return_value = mock_session

        with patch(
            "orchestrator.app.graph_builder.get_driver",
            return_value=mock_driver,
        ):
            await commit_to_neo4j(state)

        span_names = [s.name for s in telemetry.get_finished_spans()]
        assert "ingestion.commit_neo4j" in span_names


class TestQuerySpans:
    def test_classify_creates_span(self, telemetry):
        from orchestrator.app.query_engine import classify_query_node

        state = {
            "query": "What language is auth-service?",
            "max_results": 10,
            "complexity": "entity_lookup",
            "retrieval_path": "",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        classify_query_node(state)
        span_names = [s.name for s in telemetry.get_finished_spans()]
        assert "query.classify" in span_names

    @pytest.mark.asyncio
    async def test_synthesize_creates_span(self, telemetry):
        from orchestrator.app.query_engine import synthesize_answer

        state = {
            "query": "What is auth?",
            "max_results": 10,
            "complexity": "entity_lookup",
            "retrieval_path": "vector",
            "candidates": [{"name": "auth"}],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        with patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="Auth is a service.",
        ):
            await synthesize_answer(state)

        span_names = [s.name for s in telemetry.get_finished_spans()]
        assert "query.synthesize" in span_names


class TestSpanAttributes:
    def test_load_workspace_records_file_count(self, telemetry):
        from orchestrator.app.graph_builder import load_workspace_files

        state = {
            "directory_path": "",
            "raw_files": [
                {"path": "a.go", "content": "x"},
                {"path": "b.py", "content": "y"},
            ],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        load_workspace_files(state)
        spans = telemetry.get_finished_spans()
        workspace_span = next(s for s in spans if s.name == "ingestion.load_workspace")
        assert workspace_span.attributes.get("file_count") == 2

    def test_classify_records_complexity(self, telemetry):
        from orchestrator.app.query_engine import classify_query_node

        state = {
            "query": "blast radius of auth failure",
            "max_results": 10,
            "complexity": "entity_lookup",
            "retrieval_path": "",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        classify_query_node(state)
        spans = telemetry.get_finished_spans()
        classify_span = next(s for s in spans if s.name == "query.classify")
        assert classify_span.attributes.get("query.complexity") == "multi_hop"


class TestMissingIngestionSpans:
    def test_parse_manifests_creates_span(self, telemetry):
        from orchestrator.app.graph_builder import parse_k8s_and_kafka_manifests

        state = {
            "directory_path": "",
            "raw_files": [{"path": "deploy.yaml", "content": "apiVersion: v1"}],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        parse_k8s_and_kafka_manifests(state)
        span_names = [s.name for s in telemetry.get_finished_spans()]
        assert "ingestion.parse_manifests" in span_names

    @pytest.mark.asyncio
    async def test_fix_extraction_errors_creates_span(self, telemetry):
        from orchestrator.app.graph_builder import fix_extraction_errors

        state = {
            "directory_path": "",
            "raw_files": [],
            "extracted_nodes": [],
            "extraction_errors": ["bad entity"],
            "validation_retries": 0,
            "commit_status": "",
        }
        with patch(
            "orchestrator.app.graph_builder.ServiceExtractor"
        ) as mock_cls, patch.dict(
            "os.environ", {"GOOGLE_API_KEY": "test-key"}
        ):
            mock_extractor = MagicMock()
            mock_extractor.extract_all = AsyncMock(
                return_value=MagicMock(services=[], calls=[])
            )
            mock_cls.return_value = mock_extractor
            await fix_extraction_errors(state)

        span_names = [s.name for s in telemetry.get_finished_spans()]
        assert "ingestion.fix_errors" in span_names


class TestMissingQuerySpans:
    @pytest.mark.asyncio
    async def test_vector_retrieve_creates_span(self, telemetry):
        from orchestrator.app.query_engine import vector_retrieve

        state = {
            "query": "auth service",
            "max_results": 5,
            "complexity": "entity_lookup",
            "retrieval_path": "vector",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver"
        ) as mock_get:
            mock_driver = MagicMock()
            mock_driver.close = AsyncMock()
            mock_result = MagicMock()
            mock_result.data.return_value = [{"name": "auth"}]
            mock_session = AsyncMock()
            mock_session.run = AsyncMock(return_value=mock_result)
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=False)
            mock_driver.session.return_value = mock_session
            mock_get.return_value = mock_driver
            await vector_retrieve(state)

        span_names = [s.name for s in telemetry.get_finished_spans()]
        assert "query.vector_retrieve" in span_names

    @pytest.mark.asyncio
    async def test_single_hop_retrieve_creates_span(self, telemetry):
        from orchestrator.app.query_engine import single_hop_retrieve

        state = {
            "query": "auth dependencies",
            "max_results": 5,
            "complexity": "single_hop",
            "retrieval_path": "single_hop",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver"
        ) as mock_get:
            mock_driver = MagicMock()
            mock_driver.close = AsyncMock()
            mock_result = MagicMock()
            mock_result.data.return_value = [{"name": "auth"}]
            mock_session = AsyncMock()
            mock_session.run = AsyncMock(return_value=mock_result)
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=False)
            mock_driver.session.return_value = mock_session
            mock_get.return_value = mock_driver
            await single_hop_retrieve(state)

        span_names = [s.name for s in telemetry.get_finished_spans()]
        assert "query.single_hop_retrieve" in span_names

    @pytest.mark.asyncio
    async def test_cypher_retrieve_creates_span(self, telemetry):
        from orchestrator.app.query_engine import cypher_retrieve

        state = {
            "query": "blast radius of auth",
            "max_results": 5,
            "complexity": "multi_hop",
            "retrieval_path": "cypher",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver"
        ) as mock_get, patch(
            "orchestrator.app.query_engine._generate_cypher",
            new_callable=AsyncMock,
            return_value="MATCH (n) RETURN n",
        ):
            mock_driver = MagicMock()
            mock_driver.close = AsyncMock()
            mock_result = MagicMock()
            mock_result.data.return_value = [{"n": "auth"}]
            mock_session = AsyncMock()
            mock_session.run = AsyncMock(return_value=mock_result)
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=False)
            mock_driver.session.return_value = mock_session
            mock_get.return_value = mock_driver
            await cypher_retrieve(state)

        span_names = [s.name for s in telemetry.get_finished_spans()]
        assert "query.cypher_retrieve" in span_names

    @pytest.mark.asyncio
    async def test_hybrid_retrieve_creates_span(self, telemetry):
        from orchestrator.app.query_engine import hybrid_retrieve

        state = {
            "query": "aggregate service stats",
            "max_results": 5,
            "complexity": "aggregate",
            "retrieval_path": "hybrid",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver"
        ) as mock_get, patch(
            "orchestrator.app.query_engine._generate_cypher",
            new_callable=AsyncMock,
            return_value="MATCH (n) RETURN count(n)",
        ):
            mock_driver = MagicMock()
            mock_driver.close = AsyncMock()
            mock_result = MagicMock()
            mock_result.data.return_value = [{"count": 5}]
            mock_session = AsyncMock()
            mock_session.run = AsyncMock(return_value=mock_result)
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=False)
            mock_driver.session.return_value = mock_session
            mock_get.return_value = mock_driver
            await hybrid_retrieve(state)

        span_names = [s.name for s in telemetry.get_finished_spans()]
        assert "query.hybrid_retrieve" in span_names


class TestMetricsRecording:
    def test_load_workspace_records_ingestion_duration(self, telemetry):
        from orchestrator.app.graph_builder import load_workspace_files

        state = {
            "directory_path": "",
            "raw_files": [{"path": "a.go", "content": "package main"}],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        with patch(
            "orchestrator.app.graph_builder.INGESTION_DURATION"
        ) as mock_metric:
            load_workspace_files(state)
            mock_metric.record.assert_called_once()
            elapsed_ms = mock_metric.record.call_args[0][0]
            assert elapsed_ms >= 0

    @pytest.mark.asyncio
    async def test_parse_services_records_llm_extraction_duration(self, telemetry):
        from orchestrator.app.graph_builder import parse_go_and_python_services

        state = {
            "directory_path": "",
            "raw_files": [],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        with patch(
            "orchestrator.app.graph_builder.ServiceExtractor"
        ) as mock_cls, patch.dict(
            "os.environ", {"GOOGLE_API_KEY": "test-key"}
        ), patch(
            "orchestrator.app.graph_builder.LLM_EXTRACTION_DURATION"
        ) as mock_metric:
            mock_extractor = MagicMock()
            mock_extractor.extract_all = AsyncMock(
                return_value=MagicMock(services=[], calls=[])
            )
            mock_cls.return_value = mock_extractor
            await parse_go_and_python_services(state)
            mock_metric.record.assert_called_once()
            elapsed_ms = mock_metric.record.call_args[0][0]
            assert elapsed_ms >= 0

    @pytest.mark.asyncio
    async def test_commit_neo4j_records_neo4j_transaction_duration(self, telemetry):
        from orchestrator.app.graph_builder import commit_to_neo4j

        state = {
            "directory_path": "",
            "raw_files": [],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.execute_write = AsyncMock()
        mock_driver.session.return_value = mock_session

        with patch(
            "orchestrator.app.graph_builder.get_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.graph_builder.NEO4J_TRANSACTION_DURATION"
        ) as mock_metric:
            await commit_to_neo4j(state)
            mock_metric.record.assert_called_once()
            elapsed_ms = mock_metric.record.call_args[0][0]
            assert elapsed_ms >= 0

    def test_classify_records_query_duration(self, telemetry):
        from orchestrator.app.query_engine import classify_query_node

        state = {
            "query": "What language is auth-service?",
            "max_results": 10,
            "complexity": "entity_lookup",
            "retrieval_path": "",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        with patch(
            "orchestrator.app.query_engine.QUERY_DURATION"
        ) as mock_metric:
            classify_query_node(state)
            mock_metric.record.assert_called_once()
            elapsed_ms = mock_metric.record.call_args[0][0]
            assert elapsed_ms >= 0

    @pytest.mark.asyncio
    async def test_synthesize_records_query_duration(self, telemetry):
        from orchestrator.app.query_engine import synthesize_answer

        state = {
            "query": "What is auth?",
            "max_results": 10,
            "complexity": "entity_lookup",
            "retrieval_path": "vector",
            "candidates": [{"name": "auth"}],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "",
            "sources": [],
        }
        with patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="Auth is a service.",
        ), patch(
            "orchestrator.app.query_engine.QUERY_DURATION"
        ) as mock_metric:
            await synthesize_answer(state)
            mock_metric.record.assert_called_once()
            elapsed_ms = mock_metric.record.call_args[0][0]
            assert elapsed_ms >= 0


class TestSamplerConfig:
    def test_default_sampling_rate_is_ten_percent(self):
        sampler = _build_sampler()
        assert isinstance(sampler, RecordAllSampler)
        assert sampler._delegate.rate == pytest.approx(0.1)

    def test_custom_sampling_rate_from_env(self):
        with patch.dict("os.environ", {"OTEL_TRACES_SAMPLER_ARG": "0.5"}):
            sampler = _build_sampler()
            assert sampler._delegate.rate == pytest.approx(0.5)

    def test_test_exporter_uses_always_on_not_ratio(self):
        exporter = MemoryExporter()
        provider = configure_telemetry(exporter=exporter)
        root_sampler = provider.sampler._root
        assert not hasattr(root_sampler, "rate"), (
            "Test exporter path should use ALWAYS_ON, not ratio-based sampling"
        )
        provider.shutdown()

    def test_record_all_sampler_never_drops(self):
        from opentelemetry.sdk.trace.sampling import Decision

        sampler = _build_sampler(ratio=0.0)
        result = sampler.should_sample(None, 12345, "test-span")
        assert result.decision != Decision.DROP
        assert result.decision == Decision.RECORD_ONLY


class TestErrorForceExportProcessor:
    def _make_provider_with_zero_sample_rate(self, exporter):
        sampler = _build_sampler(ratio=0.0)
        provider = TracerProvider(sampler=sampler)
        provider.add_span_processor(ErrorForceExportProcessor(exporter))
        return provider

    def test_unsampled_error_span_force_exported(self):
        from opentelemetry.trace import StatusCode

        exporter = MemoryExporter()
        provider = self._make_provider_with_zero_sample_rate(exporter)
        tracer = provider.get_tracer("test")

        with tracer.start_as_current_span("error-op") as span:
            span.set_status(StatusCode.ERROR, "something broke")

        exported = exporter.get_finished_spans()
        error_spans = [s for s in exported if s.name == "error-op"]
        assert len(error_spans) == 1
        assert error_spans[0].status.status_code == StatusCode.ERROR
        provider.shutdown()

    def test_unsampled_success_span_not_exported(self):
        from opentelemetry.trace import StatusCode

        exporter = MemoryExporter()
        provider = self._make_provider_with_zero_sample_rate(exporter)
        tracer = provider.get_tracer("test")

        with tracer.start_as_current_span("ok-op") as span:
            span.set_status(StatusCode.OK)

        exported = exporter.get_finished_spans()
        ok_spans = [s for s in exported if s.name == "ok-op"]
        assert len(ok_spans) == 0
        provider.shutdown()

    def test_unsampled_unset_status_not_exported(self):
        exporter = MemoryExporter()
        provider = self._make_provider_with_zero_sample_rate(exporter)
        tracer = provider.get_tracer("test")

        with tracer.start_as_current_span("neutral-op"):
            pass

        exported = exporter.get_finished_spans()
        neutral_spans = [s for s in exported if s.name == "neutral-op"]
        assert len(neutral_spans) == 0
        provider.shutdown()

    def test_sampled_error_span_not_duplicate_exported(self):
        from opentelemetry.trace import StatusCode

        exporter = MemoryExporter()
        sampler = _build_sampler(ratio=1.0)
        provider = TracerProvider(sampler=sampler)
        provider.add_span_processor(ErrorForceExportProcessor(exporter))
        tracer = provider.get_tracer("test")

        with tracer.start_as_current_span("error-op") as span:
            span.set_status(StatusCode.ERROR, "broke")

        exported = exporter.get_finished_spans()
        assert len([s for s in exported if s.name == "error-op"]) == 0
        provider.shutdown()

    def test_production_telemetry_includes_error_processor(self):
        with patch.dict("os.environ", {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4317"}):
            provider = configure_telemetry()
            processor_types = [
                type(sp).__name__ for sp in provider._active_span_processor._span_processors
            ]
            assert "ErrorForceExportProcessor" in processor_types
            provider.shutdown()

    def test_production_telemetry_uses_record_all_sampler(self):
        with patch.dict("os.environ", {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4317"}):
            provider = configure_telemetry()
            assert isinstance(provider.sampler, RecordAllSampler)
            provider.shutdown()
