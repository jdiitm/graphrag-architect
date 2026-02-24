from __future__ import annotations

import asyncio
from typing import Any, Callable, Coroutine, Dict, Generator, List

from google.api_core.exceptions import ResourceExhausted, ServiceUnavailable
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from orchestrator.app.config import ExtractionConfig
from orchestrator.app.extraction_models import (
    CallsEdge,
    ServiceExtractionResult,
    ServiceNode,
)
from orchestrator.app.prompt_sanitizer import sanitize_source_content

SYSTEM_PROMPT = (
    "You are a distributed-systems code analyst. Given source files from a "
    "codebase, extract ONLY the following:\n\n"
    "1. Services - each distinct microservice, API server, or standalone "
    "application.\n"
    "   Fields: id (kebab-case derived from module/package name), name, "
    'language ("go" or "python"), framework (e.g. "gin", "echo", "fastapi", '
    '"flask"), opentelemetry_enabled (true if any OTel import or dependency '
    "is present).\n\n"
    "2. Calls - explicit inter-service HTTP or gRPC calls found in the code.\n"
    "   Fields: source_service_id, target_service_id, protocol "
    '("http", "grpc").\n\n'
    "Rules:\n"
    "- Only extract entities with concrete evidence in the source code.\n"
    "- If a service name is ambiguous, derive the id from the module or "
    "package name.\n"
    "- Do NOT invent services or calls that are not explicitly present.\n"
    "- If no services or calls are found, return empty lists."
)

HUMAN_PROMPT_TEMPLATE = (
    "Analyze the following source files and extract all services and "
    "inter-service calls.\n\n{file_contents}"
)


class ServiceExtractor:
    chain: Callable[..., Coroutine[Any, Any, ServiceExtractionResult]]
    config: ExtractionConfig

    def __init__(self, config: ExtractionConfig) -> None:
        self.config = config
        llm = ChatGoogleGenerativeAI(
            model=config.model_name,
            google_api_key=config.google_api_key,
        )
        structured_llm = llm.with_structured_output(ServiceExtractionResult)

        @retry(
            wait=wait_exponential(
                min=config.retry_min_wait, max=config.retry_max_wait
            ),
            stop=stop_after_attempt(config.max_retries),
            retry=retry_if_exception_type((ResourceExhausted, ServiceUnavailable)),
        )
        async def _invoke_with_retry(
            messages: list,
        ) -> ServiceExtractionResult:
            return await structured_llm.ainvoke(messages)

        self.chain = _invoke_with_retry

    @staticmethod
    def filter_source_files(
        raw_files: List[Dict[str, str]],
    ) -> List[Dict[str, str]]:
        return [f for f in raw_files if f["path"].endswith((".go", ".py"))]

    @staticmethod
    def batch_by_token_budget(
        files: List[Dict[str, str]], budget: int
    ) -> Generator[List[Dict[str, str]], None, None]:
        current_batch: List[Dict[str, str]] = []
        current_tokens = 0

        for file_entry in files:
            estimated_tokens = len(file_entry["content"]) // 4
            if current_batch and current_tokens + estimated_tokens > budget:
                yield current_batch
                current_batch = []
                current_tokens = 0
            current_batch.append(file_entry)
            current_tokens += estimated_tokens

        if current_batch:
            yield current_batch

    async def extract_batch(
        self, files: List[Dict[str, str]]
    ) -> ServiceExtractionResult:
        file_contents = "\n".join(
            f"--- FILE: {f['path']} ---\n"
            f"{sanitize_source_content(f['content'], f['path'])}"
            for f in files
        )
        messages = [
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(
                content=HUMAN_PROMPT_TEMPLATE.format(file_contents=file_contents)
            ),
        ]
        return await self.chain(messages)

    async def extract_all(
        self, raw_files: List[Dict[str, str]]
    ) -> ServiceExtractionResult:
        source_files = self.filter_source_files(raw_files)

        if not source_files:
            return ServiceExtractionResult(services=[], calls=[])

        batches = list(
            self.batch_by_token_budget(
                source_files, self.config.token_budget_per_batch
            )
        )
        semaphore = asyncio.Semaphore(self.config.max_concurrency)

        async def _process_batch(
            batch: List[Dict[str, str]],
        ) -> ServiceExtractionResult:
            async with semaphore:
                return await self.extract_batch(batch)

        results: list[ServiceExtractionResult] = list(
            await asyncio.gather(*(_process_batch(b) for b in batches))
        )

        seen_service_ids: set[str] = set()
        unique_services: List[ServiceNode] = []
        all_calls: List[CallsEdge] = []

        for result in results:
            for service in result.services:
                if service.id not in seen_service_ids:
                    seen_service_ids.add(service.id)
                    unique_services.append(service)
            all_calls.extend(result.calls)

        return ServiceExtractionResult(services=unique_services, calls=all_calls)
