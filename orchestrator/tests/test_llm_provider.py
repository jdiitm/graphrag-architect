import pytest

from orchestrator.app.config import ExtractionConfig
from orchestrator.app.extraction_models import (
    CallsEdge,
    ServiceExtractionResult,
    ServiceNode,
)
from orchestrator.app.llm_provider import (
    GeminiProvider,
    StubProvider,
    create_provider,
)


def _minimal_config() -> ExtractionConfig:
    return ExtractionConfig(google_api_key="test-key-for-unit-tests")


class TestCreateProvider:
    def test_returns_gemini_provider_for_gemini(self) -> None:
        provider = create_provider("gemini", _minimal_config())
        assert isinstance(provider, GeminiProvider)

    def test_returns_gemini_provider_for_default(self) -> None:
        provider = create_provider("", _minimal_config())
        assert isinstance(provider, GeminiProvider)

    def test_returns_stub_provider_for_stub(self) -> None:
        provider = create_provider("stub", _minimal_config())
        assert isinstance(provider, StubProvider)

    def test_raises_value_error_for_unknown_provider(self) -> None:
        with pytest.raises(ValueError, match="Unknown LLM provider"):
            create_provider("unknown", _minimal_config())

    def test_raises_value_error_for_unknown_provider_case_insensitive(
        self,
    ) -> None:
        with pytest.raises(ValueError, match="Unknown LLM provider"):
            create_provider("CLAUDE", _minimal_config())


class TestStubProvider:
    @pytest.mark.asyncio
    async def test_ainvoke_returns_configured_response(self) -> None:
        stub = StubProvider(ainvoke_response="stub-text-output")
        result = await stub.ainvoke("any prompt")
        assert result == "stub-text-output"

    @pytest.mark.asyncio
    async def test_ainvoke_structured_returns_configured_response(
        self,
    ) -> None:
        configured = ServiceExtractionResult(
            services=[
                ServiceNode(
                    id="svc-a",
                    name="Service A",
                    language="go",
                    framework="gin",
                    opentelemetry_enabled=True,
                )
            ],
            calls=[
                CallsEdge(
                    source_service_id="svc-a",
                    target_service_id="svc-b",
                    protocol="http",
                )
            ],
        )
        stub = StubProvider(ainvoke_structured_response=configured)
        result = await stub.ainvoke_structured("prompt", [])
        assert result == configured
        assert result.services[0].id == "svc-a"
        assert result.calls[0].source_service_id == "svc-a"


class TestGeminiProvider:
    def test_instantiation_with_config(self) -> None:
        config = _minimal_config()
        provider = GeminiProvider(config)
        assert provider._config is config
