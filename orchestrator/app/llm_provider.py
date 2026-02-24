from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI

from orchestrator.app.config import ExtractionConfig
from orchestrator.app.extraction_models import ServiceExtractionResult


@runtime_checkable
class LLMProvider(Protocol):
    async def ainvoke(self, prompt: str) -> str: ...

    async def ainvoke_structured(self, prompt: str, messages: list) -> Any: ...


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

    async def ainvoke_structured(self, prompt: str, messages: list) -> Any:
        return self._ainvoke_structured_response


def create_provider(
    provider_name: str, config: ExtractionConfig
) -> LLMProvider:
    name = (provider_name or "gemini").strip().lower()
    if name == "gemini":
        return GeminiProvider(config)
    if name == "stub":
        return StubProvider()
    raise ValueError(f"Unknown LLM provider: {provider_name!r}")
