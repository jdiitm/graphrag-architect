from pydantic import BaseModel, ValidationError
import pytest

from orchestrator.tests.contract.contracts import (
    ENDPOINT_CONTRACTS,
    STATUS_CODE_RETRY_MAP,
    EndpointContract,
    ErrorResponse,
    ExpectedAsyncResponse,
    ExpectedHealthResponse,
    ExpectedIngestResponse,
    GoIngestDocument,
    GoIngestRequest,
    RetryBehavior,
)


class TestContractSchemasAreValidPydantic:
    def test_go_ingest_document_is_pydantic(self):
        assert issubclass(GoIngestDocument, BaseModel)

    def test_go_ingest_request_is_pydantic(self):
        assert issubclass(GoIngestRequest, BaseModel)

    def test_expected_ingest_response_is_pydantic(self):
        assert issubclass(ExpectedIngestResponse, BaseModel)

    def test_expected_async_response_is_pydantic(self):
        assert issubclass(ExpectedAsyncResponse, BaseModel)

    def test_expected_health_response_is_pydantic(self):
        assert issubclass(ExpectedHealthResponse, BaseModel)

    def test_error_response_is_pydantic(self):
        assert issubclass(ErrorResponse, BaseModel)


class TestContractSchemasCanInstantiate:
    def test_go_ingest_document_round_trips(self):
        doc = GoIngestDocument(
            file_path="main.go",
            content="cGFja2FnZSBtYWlu",
            source_type="source_code",
        )
        data = doc.model_dump()
        assert data["file_path"] == "main.go"
        assert data["source_type"] == "source_code"

    def test_go_ingest_document_optional_fields(self):
        doc = GoIngestDocument(
            file_path="main.go",
            content="cGFja2FnZSBtYWlu",
            source_type="source_code",
            repository="myrepo",
            commit_sha="abc123",
        )
        data = doc.model_dump()
        assert data["repository"] == "myrepo"
        assert data["commit_sha"] == "abc123"

    def test_go_ingest_document_omits_optional_when_none(self):
        doc = GoIngestDocument(
            file_path="main.go",
            content="cGFja2FnZSBtYWlu",
            source_type="source_code",
        )
        data = doc.model_dump(exclude_none=True)
        assert "repository" not in data
        assert "commit_sha" not in data

    def test_go_ingest_request_round_trips(self):
        req = GoIngestRequest(
            documents=[
                GoIngestDocument(
                    file_path="main.go",
                    content="cGFja2FnZSBtYWlu",
                    source_type="source_code",
                ),
            ],
        )
        data = req.model_dump()
        assert len(data["documents"]) == 1

    def test_expected_ingest_response_round_trips(self):
        resp = ExpectedIngestResponse(
            status="committed",
            entities_extracted=5,
            errors=[],
        )
        data = resp.model_dump()
        assert data["status"] == "committed"
        assert data["entities_extracted"] == 5

    def test_expected_async_response_round_trips(self):
        resp = ExpectedAsyncResponse(
            job_id="abc-123",
            status="pending",
        )
        data = resp.model_dump()
        assert data["job_id"] == "abc-123"

    def test_expected_health_response_round_trips(self):
        resp = ExpectedHealthResponse(status="healthy")
        assert resp.model_dump()["status"] == "healthy"


class TestEndpointContractsCompleteness:
    def test_ingest_sync_defined(self):
        assert "ingest_sync" in ENDPOINT_CONTRACTS

    def test_ingest_async_defined(self):
        assert "ingest_async" in ENDPOINT_CONTRACTS

    def test_health_defined(self):
        assert "health" in ENDPOINT_CONTRACTS

    def test_ingest_sync_is_post(self):
        assert ENDPOINT_CONTRACTS["ingest_sync"].method == "POST"

    def test_ingest_sync_path(self):
        assert ENDPOINT_CONTRACTS["ingest_sync"].path == "/ingest"

    def test_ingest_async_is_post(self):
        assert ENDPOINT_CONTRACTS["ingest_async"].method == "POST"

    def test_health_is_get(self):
        assert ENDPOINT_CONTRACTS["health"].method == "GET"

    def test_health_path(self):
        assert ENDPOINT_CONTRACTS["health"].path == "/health"

    def test_ingest_sync_success_status(self):
        assert ENDPOINT_CONTRACTS["ingest_sync"].success_status == 200

    def test_ingest_async_success_status(self):
        assert ENDPOINT_CONTRACTS["ingest_async"].success_status == 202

    def test_health_success_status(self):
        assert ENDPOINT_CONTRACTS["health"].success_status == 200

    def test_all_contracts_have_content_type(self):
        for name, contract in ENDPOINT_CONTRACTS.items():
            assert contract.content_type == "application/json", (
                f"{name} missing content_type"
            )

    def test_all_contracts_are_endpoint_contract_type(self):
        for name, contract in ENDPOINT_CONTRACTS.items():
            assert isinstance(contract, EndpointContract), (
                f"{name} is not EndpointContract"
            )


class TestStatusCodeMappingsComplete:
    REQUIRED_CODES = {400, 413, 422, 429, 500, 503}

    def test_all_error_codes_mapped(self):
        assert self.REQUIRED_CODES.issubset(STATUS_CODE_RETRY_MAP.keys())

    def test_retryable_set(self):
        retryable = {
            code for code, behavior in STATUS_CODE_RETRY_MAP.items()
            if behavior == RetryBehavior.RETRY
        }
        assert retryable == {429, 503}

    def test_dlq_set_includes_required(self):
        dlq = {
            code for code, behavior in STATUS_CODE_RETRY_MAP.items()
            if behavior == RetryBehavior.DLQ
        }
        assert {400, 413, 422, 500}.issubset(dlq)

    def test_no_unknown_behaviors(self):
        valid = {RetryBehavior.RETRY, RetryBehavior.DLQ}
        for code, behavior in STATUS_CODE_RETRY_MAP.items():
            assert behavior in valid, f"Unknown behavior for {code}: {behavior}"
