from __future__ import annotations

import logging
import time
from typing import Any, List, Optional, Protocol, runtime_checkable

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI

from orchestrator.app.config import ExtractionConfig
from orchestrator.app.extraction_models import ServiceExtractionResult

_logger = logging.getLogger(__name__)


class LLMError(Exception):
    pass


class ProviderUnavailableError(LLMError):
    pass


class ProviderTimeoutError(LLMError):
    pass


@runtime_checkable
class LLMProvider(Protocol):
    async def ainvoke(self, prompt: str) -> str: ...

    async def ainvoke_structured(self, prompt: str, messages: list) -> Any: ...

    async def ainvoke_messages(self, messages: list) -> str: ...


class GeminiProvider:
    def __init__(self, config: ExtractionConfig) -> None:
        self._config = config
        self._llm = ChatGoogleGenerativeAI(
            model=config.model_name,
            google_api_key=config.google_api_key,
        )
        self._structured_llm = self._llm.with_structured_output(
            ServiceExtractionResult
        )

    async def ainvoke(self, prompt: str) -> str:
        response = await self._llm.ainvoke(prompt)
        return str(response.content)

    async def ainvoke_messages(self, messages: list) -> str:
        response = await self._llm.ainvoke(messages)
        return str(response.content)

    async def ainvoke_structured(self, prompt: str, messages: list) -> Any:
        lc_messages: list[BaseMessage] = []
        for msg in messages:
            if isinstance(msg, BaseMessage):
                lc_messages.append(msg)
            else:
                lc_messages.append(HumanMessage(content=str(msg)))
        if prompt.strip():
            lc_messages.insert(0, SystemMessage(content=prompt))
        return await self._structured_llm.ainvoke(lc_messages)


class StubProvider:
    def __init__(
        self,
        ainvoke_response: str = "",
        ainvoke_structured_response: Any = None,
    ) -> None:
        self._ainvoke_response = ainvoke_response
        self._ainvoke_structured_response = ainvoke_structured_response

    async def ainvoke(self, prompt: str) -> str:
        return self._ainvoke_response

    async def ainvoke_messages(self, messages: list) -> str:
        return self._ainvoke_response

    async def ainvoke_structured(self, prompt: str, messages: list) -> Any:
        return self._ainvoke_structured_response


class ProviderWithCircuitBreaker:
    def __init__(
        self,
        inner: Any,
        failure_threshold: int = 5,
        reset_timeout: float = 60.0,
    ) -> None:
        self._inner = inner
        self._failure_threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._failure_count = 0
        self._last_failure_time: float = 0.0
        self._open = False

    def _check_state(self) -> None:
        if self._open:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self._reset_timeout:
                self._open = False
                self._failure_count = 0
            else:
                raise ProviderUnavailableError(
                    f"circuit open; retry after {self._reset_timeout - elapsed:.0f}s"
                )

    def _record_failure(self, exc: Exception) -> None:
        self._failure_count += 1
        self._last_failure_time = time.monotonic()
        if self._failure_count >= self._failure_threshold:
            self._open = True

    def _record_success(self) -> None:
        self._failure_count = 0
        self._open = False

    async def ainvoke(self, prompt: str) -> str:
        self._check_state()
        try:
            result = await self._inner.ainvoke(prompt)
            self._record_success()
            return result
        except Exception as exc:
            self._record_failure(exc)
            raise

    async def ainvoke_messages(self, messages: list) -> str:
        self._check_state()
        try:
            result = await self._inner.ainvoke_messages(messages)
            self._record_success()
            return result
        except Exception as exc:
            self._record_failure(exc)
            raise

    async def ainvoke_structured(self, prompt: str, messages: list) -> Any:
        self._check_state()
        try:
            result = await self._inner.ainvoke_structured(prompt, messages)
            self._record_success()
            return result
        except Exception as exc:
            self._record_failure(exc)
            raise


class FallbackChain:
    def __init__(self, providers: List[Any]) -> None:
        self._providers = providers

    async def ainvoke(self, prompt: str) -> str:
        last_error: Optional[Exception] = None
        for provider in self._providers:
            try:
                return await provider.ainvoke(prompt)
            except Exception as exc:
                _logger.warning("Provider %s failed: %s", type(provider).__name__, exc)
                last_error = exc
        raise LLMError(f"All {len(self._providers)} providers failed") from last_error

    async def ainvoke_messages(self, messages: list) -> str:
        last_error: Optional[Exception] = None
        for provider in self._providers:
            try:
                return await provider.ainvoke_messages(messages)
            except Exception as exc:
                _logger.warning("Provider %s failed: %s", type(provider).__name__, exc)
                last_error = exc
        raise LLMError(f"All {len(self._providers)} providers failed") from last_error

    async def ainvoke_structured(self, prompt: str, messages: list) -> Any:
        last_error: Optional[Exception] = None
        for provider in self._providers:
            try:
                return await provider.ainvoke_structured(prompt, messages)
            except Exception as exc:
                _logger.warning("Provider %s failed: %s", type(provider).__name__, exc)
                last_error = exc
        raise LLMError(f"All {len(self._providers)} providers failed") from last_error


class ClaudeProvider:
    def __init__(self, config: ExtractionConfig) -> None:
        self._config = config
        try:
            from langchain_anthropic import ChatAnthropic
            self._llm = ChatAnthropic(
                model=config.claude_model_name,
                anthropic_api_key=config.anthropic_api_key,
            )
        except ImportError as exc:
            raise ImportError(
                "langchain-anthropic is required for ClaudeProvider. "
                "Install with: pip install langchain-anthropic"
            ) from exc

    async def ainvoke(self, prompt: str) -> str:
        response = await self._llm.ainvoke(prompt)
        return str(response.content)

    async def ainvoke_messages(self, messages: list) -> str:
        response = await self._llm.ainvoke(messages)
        return str(response.content)

    async def ainvoke_structured(self, prompt: str, messages: list) -> Any:
        lc_messages: list[BaseMessage] = []
        for msg in messages:
            if isinstance(msg, BaseMessage):
                lc_messages.append(msg)
            else:
                lc_messages.append(HumanMessage(content=str(msg)))
        if prompt.strip():
            lc_messages.insert(0, SystemMessage(content=prompt))
        return await self._llm.ainvoke(lc_messages)


def create_provider(
    provider_name: str, config: ExtractionConfig
) -> LLMProvider:
    name = (provider_name or "gemini").strip().lower()
    if name == "gemini":
        inner = GeminiProvider(config)
        return ProviderWithCircuitBreaker(inner)
    if name == "claude":
        inner = ClaudeProvider(config)
        return ProviderWithCircuitBreaker(inner)
    if name == "stub":
        return StubProvider()
    raise ValueError(f"Unknown LLM provider: {provider_name!r}")


def create_provider_with_failover(config: ExtractionConfig) -> LLMProvider:
    primary_name = (config.llm_provider or "gemini").strip().lower()
    fallback_name = "claude" if primary_name == "gemini" else "gemini"

    providers: list[Any] = []
    try:
        primary = create_provider(primary_name, config)
        providers.append(primary)
    except (ValueError, ImportError):
        _logger.warning("Primary provider %r unavailable", primary_name)

    try:
        fallback = create_provider(fallback_name, config)
        providers.append(fallback)
    except (ValueError, ImportError):
        _logger.warning("Fallback provider %r unavailable", fallback_name)

    if not providers:
        raise LLMError("No LLM providers available")

    if len(providers) == 1:
        return providers[0]

    return FallbackChain(providers=providers)
