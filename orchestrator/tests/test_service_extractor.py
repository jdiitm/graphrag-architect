from __future__ import annotations

from typing import List, Dict
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.extraction_models import (
    CallsEdge,
    ServiceExtractionResult,
    ServiceNode,
)
from orchestrator.app.llm_extraction import ServiceExtractor


GO_GIN_POSTGRES_SNIPPET = """\
package main

import (
    "database/sql"
    "log"
    "net/http"

    "github.com/gin-gonic/gin"
    _ "github.com/lib/pq"
    "go.opentelemetry.io/otel"
)

func main() {
    db, err := sql.Open("postgres", "host=localhost dbname=orders sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    tracer := otel.Tracer("order-service")
    _ = tracer

    r := gin.Default()
    r.GET("/orders", func(c *gin.Context) {
        rows, _ := db.Query("SELECT id, total FROM orders")
        defer rows.Close()
        c.JSON(http.StatusOK, gin.H{"status": "ok"})
    })
    r.Run(":8080")
}
"""

PYTHON_FASTAPI_HTTP_SNIPPET = """\
from fastapi import FastAPI
import httpx
from opentelemetry import trace

app = FastAPI()
tracer = trace.get_tracer("user-service")

@app.get("/users/{user_id}")
async def get_user(user_id: str):
    with tracer.start_as_current_span("fetch-orders"):
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"http://order-service:8080/orders?user={user_id}"
            )
    return {"user_id": user_id, "orders": resp.json()}
"""


EXPECTED_ORDER_SERVICE = ServiceNode(
    id="order-service",
    name="order-service",
    language="go",
    framework="gin",
    opentelemetry_enabled=True,
)

EXPECTED_USER_SERVICE = ServiceNode(
    id="user-service",
    name="user-service",
    language="python",
    framework="fastapi",
    opentelemetry_enabled=True,
)

EXPECTED_HTTP_CALL = CallsEdge(
    source_service_id="user-service",
    target_service_id="order-service",
    protocol="http",
)


@pytest.fixture
def raw_files_mixed() -> List[Dict[str, str]]:
    return [
        {"path": "services/order-service/main.go", "content": GO_GIN_POSTGRES_SNIPPET},
        {"path": "services/user-service/main.py", "content": PYTHON_FASTAPI_HTTP_SNIPPET},
        {"path": "infra/k8s/deployment.yaml", "content": "apiVersion: apps/v1"},
        {"path": "docs/architecture.md", "content": "# Architecture"},
        {"path": "config/settings.json", "content": "{}"},
    ]


@pytest.fixture
def raw_files_go_only() -> List[Dict[str, str]]:
    return [
        {"path": "services/order-service/main.go", "content": GO_GIN_POSTGRES_SNIPPET},
    ]


@pytest.fixture
def raw_files_python_only() -> List[Dict[str, str]]:
    return [
        {"path": "services/user-service/main.py", "content": PYTHON_FASTAPI_HTTP_SNIPPET},
    ]


class TestFilterSourceFiles:

    def test_keeps_go_and_py(self, raw_files_mixed: List[Dict[str, str]]) -> None:
        result = ServiceExtractor.filter_source_files(raw_files_mixed)
        paths = [f["path"] for f in result]
        assert len(result) == 2
        assert "services/order-service/main.go" in paths
        assert "services/user-service/main.py" in paths

    def test_excludes_non_source(self, raw_files_mixed: List[Dict[str, str]]) -> None:
        result = ServiceExtractor.filter_source_files(raw_files_mixed)
        paths = [f["path"] for f in result]
        assert "infra/k8s/deployment.yaml" not in paths
        assert "docs/architecture.md" not in paths
        assert "config/settings.json" not in paths

    def test_empty_input(self) -> None:
        result = ServiceExtractor.filter_source_files([])
        assert result == []


class TestBatchByTokenBudget:

    def test_single_batch_within_budget(
        self, raw_files_go_only: List[Dict[str, str]]
    ) -> None:
        batches = list(
            ServiceExtractor.batch_by_token_budget(raw_files_go_only, budget=100_000)
        )
        assert len(batches) == 1
        assert len(batches[0]) == 1

    def test_splits_when_exceeding_budget(
        self, raw_files_mixed: List[Dict[str, str]]
    ) -> None:
        source_files = ServiceExtractor.filter_source_files(raw_files_mixed)
        batches = list(
            ServiceExtractor.batch_by_token_budget(source_files, budget=50)
        )
        assert len(batches) >= 2

    def test_empty_input(self) -> None:
        batches = list(ServiceExtractor.batch_by_token_budget([], budget=100_000))
        assert batches == []


class TestExtractBatch:

    @pytest.mark.asyncio
    async def test_go_service_extraction(
        self, raw_files_go_only: List[Dict[str, str]]
    ) -> None:
        mock_result = ServiceExtractionResult(
            services=[EXPECTED_ORDER_SERVICE],
            calls=[],
        )

        extractor = ServiceExtractor.__new__(ServiceExtractor)
        extractor.chain = AsyncMock(return_value=mock_result)

        result = await extractor.extract_batch(raw_files_go_only)

        assert len(result.services) == 1
        service = result.services[0]
        assert service.id == "order-service"
        assert service.language == "go"
        assert service.framework == "gin"
        assert service.opentelemetry_enabled is True

    @pytest.mark.asyncio
    async def test_python_service_with_call(
        self, raw_files_python_only: List[Dict[str, str]]
    ) -> None:
        mock_result = ServiceExtractionResult(
            services=[EXPECTED_USER_SERVICE],
            calls=[EXPECTED_HTTP_CALL],
        )

        extractor = ServiceExtractor.__new__(ServiceExtractor)
        extractor.chain = AsyncMock(return_value=mock_result)

        result = await extractor.extract_batch(raw_files_python_only)

        assert len(result.services) == 1
        assert result.services[0].id == "user-service"
        assert result.services[0].framework == "fastapi"

        assert len(result.calls) == 1
        call = result.calls[0]
        assert call.source_service_id == "user-service"
        assert call.target_service_id == "order-service"
        assert call.protocol == "http"


class TestExtractAll:

    @pytest.mark.asyncio
    async def test_aggregates_across_batches(
        self, raw_files_mixed: List[Dict[str, str]]
    ) -> None:
        batch_results = [
            ServiceExtractionResult(
                services=[EXPECTED_ORDER_SERVICE],
                calls=[],
            ),
            ServiceExtractionResult(
                services=[EXPECTED_USER_SERVICE],
                calls=[EXPECTED_HTTP_CALL],
            ),
        ]

        extractor = ServiceExtractor.__new__(ServiceExtractor)
        extractor.chain = AsyncMock(side_effect=batch_results)
        extractor.config = type("Cfg", (), {
            "max_concurrency": 2,
            "token_budget_per_batch": 50,
        })()

        result = await extractor.extract_all(raw_files_mixed)

        service_ids = {s.id for s in result.services}
        assert "order-service" in service_ids
        assert "user-service" in service_ids
        assert len(result.calls) == 1

    @pytest.mark.asyncio
    async def test_deduplicates_services_by_id(self) -> None:
        duplicate_results = [
            ServiceExtractionResult(
                services=[EXPECTED_ORDER_SERVICE],
                calls=[],
            ),
            ServiceExtractionResult(
                services=[EXPECTED_ORDER_SERVICE],
                calls=[],
            ),
        ]

        files = [
            {"path": "a/main.go", "content": "x" * 200},
            {"path": "b/main.go", "content": "y" * 200},
        ]

        extractor = ServiceExtractor.__new__(ServiceExtractor)
        extractor.chain = AsyncMock(side_effect=duplicate_results)
        extractor.config = type("Cfg", (), {
            "max_concurrency": 2,
            "token_budget_per_batch": 60,
        })()

        result = await extractor.extract_all(files)

        assert len(result.services) == 1
        assert result.services[0].id == "order-service"

    @pytest.mark.asyncio
    async def test_empty_files_returns_empty_result(self) -> None:
        extractor = ServiceExtractor.__new__(ServiceExtractor)
        extractor.config = type("Cfg", (), {
            "max_concurrency": 2,
            "token_budget_per_batch": 100_000,
        })()

        result = await extractor.extract_all([])

        assert result.services == []
        assert result.calls == []
