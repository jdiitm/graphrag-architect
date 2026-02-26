import pytest

from orchestrator.app.ast_extraction import GoASTExtractor


GO_HTTP_SERVER = '''package main

import (
    "net/http"
    "log"
)

func main() {
    http.HandleFunc("/health", healthHandler)
    log.Fatal(http.ListenAndServe(":8080", nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
    w.Write([]byte("ok"))
}
'''

GO_HTTP_CLIENT = '''package processor

import (
    "bytes"
    "context"
    "net/http"
)

type ForwardingProcessor struct {
    orchestratorURL string
    client          *http.Client
}

func (f *ForwardingProcessor) Process(ctx context.Context) error {
    req, _ := http.NewRequestWithContext(ctx, http.MethodPost, f.orchestratorURL+"/ingest", bytes.NewReader(nil))
    resp, err := f.client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    return nil
}
'''

GO_MULTI_HTTP_CALLS = '''package gateway

import (
    "net/http"
)

func FanOut() {
    http.Get("http://auth-svc/verify")
    http.Post("http://billing-svc/charge", "application/json", nil)
    http.Head("http://inventory-svc/status")
}
'''

GO_KAFKA_CONSUMER = '''package main

import (
    "github.com/twmb/franz-go/pkg/kgo"
    "context"
)

func main() {
    client, _ := kgo.NewClient(
        kgo.SeedBrokers("localhost:9092"),
        kgo.ConsumeTopics("raw-documents"),
        kgo.ConsumerGroup("ingestion-workers"),
    )
    defer client.Close()
    for {
        fetches := client.PollFetches(context.Background())
        _ = fetches
    }
}
'''

GO_GRPC_SERVER = '''package main

import (
    "google.golang.org/grpc"
    "net"
    "log"
)

func main() {
    lis, _ := net.Listen("tcp", ":50051")
    s := grpc.NewServer()
    log.Fatal(s.Serve(lis))
}
'''

GO_NO_SERVICE = '''package utils

func Add(a, b int) int {
    return a + b
}
'''

PYTHON_CODE = '''from fastapi import FastAPI

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}
'''


class TestGoASTExtractorServiceDetection:
    def test_detects_http_server(self):
        extractor = GoASTExtractor()
        result = extractor.extract("main.go", GO_HTTP_SERVER)
        assert len(result.services) >= 1
        service = result.services[0]
        assert service.language == "go"

    def test_detects_grpc_server(self):
        extractor = GoASTExtractor()
        result = extractor.extract("main.go", GO_GRPC_SERVER)
        assert len(result.services) >= 1
        service = result.services[0]
        assert service.language == "go"
        assert service.framework == "grpc"

    def test_no_service_for_utility_package(self):
        extractor = GoASTExtractor()
        result = extractor.extract("utils.go", GO_NO_SERVICE)
        assert len(result.services) == 0

    def test_ignores_python_source(self):
        extractor = GoASTExtractor()
        result = extractor.extract("app.py", PYTHON_CODE)
        assert len(result.services) == 0
        assert len(result.calls) == 0


class TestGoASTExtractorCallDetection:
    def test_detects_http_client_call(self):
        extractor = GoASTExtractor()
        result = extractor.extract("forwarding.go", GO_HTTP_CLIENT)
        assert len(result.calls) >= 1
        call = result.calls[0]
        assert call.protocol == "http"

    def test_detects_http_new_request(self):
        extractor = GoASTExtractor()
        result = extractor.extract("forwarding.go", GO_HTTP_CLIENT)
        calls_with_http = [c for c in result.calls if c.protocol == "http"]
        assert len(calls_with_http) >= 1

    def test_captures_all_http_calls_not_just_first(self):
        extractor = GoASTExtractor()
        result = extractor.extract("services/gateway/main.go", GO_MULTI_HTTP_CALLS)
        targets = [c.target_hint for c in result.calls]
        assert len(result.calls) >= 3, (
            f"Expected at least 3 HTTP calls but got {len(result.calls)}: {targets}"
        )
        assert any("auth-svc" in t for t in targets), f"Missing auth-svc in {targets}"
        assert any("billing-svc" in t for t in targets), f"Missing billing-svc in {targets}"
        assert any("inventory-svc" in t for t in targets), f"Missing inventory-svc in {targets}"


class TestGoASTExtractorKafka:
    def test_detects_kafka_consumer_topic(self):
        extractor = GoASTExtractor()
        result = extractor.extract("main.go", GO_KAFKA_CONSUMER)
        assert len(result.topics_consumed) >= 1
        assert "raw-documents" in result.topics_consumed


class TestASTExtractionResultBridge:
    def test_to_extraction_result_maps_services(self):
        extractor = GoASTExtractor()
        result = extractor.extract("cmd/main.go", GO_HTTP_SERVER)
        converted = result.to_extraction_result()
        assert len(converted.services) == len(result.services)
        assert converted.services[0].id == result.services[0].service_id
        assert converted.services[0].language == "go"

    def test_to_extraction_result_maps_calls(self):
        extractor = GoASTExtractor()
        result = extractor.extract("services/gateway/main.go", GO_MULTI_HTTP_CALLS)
        converted = result.to_extraction_result()
        assert len(converted.calls) == len(result.calls)
        for orig, mapped in zip(result.calls, converted.calls):
            assert mapped.source_service_id == orig.source_service_id
            assert mapped.target_service_id == orig.target_hint
            assert mapped.protocol == orig.protocol

    def test_confidence_defaults_to_one(self):
        extractor = GoASTExtractor()
        result = extractor.extract("cmd/main.go", GO_HTTP_SERVER)
        assert result.services[0].confidence == 1.0
        call_result = extractor.extract("services/gateway/main.go", GO_MULTI_HTTP_CALLS)
        assert all(c.confidence == 1.0 for c in call_result.calls)


class TestGoASTExtractorBatchExtraction:
    def test_extract_all_processes_multiple_files(self):
        extractor = GoASTExtractor()
        files = [
            {"path": "cmd/main.go", "content": GO_HTTP_SERVER},
            {"path": "internal/processor/forwarding.go", "content": GO_HTTP_CLIENT},
            {"path": "utils/math.go", "content": GO_NO_SERVICE},
        ]
        result = extractor.extract_all(files)
        assert len(result.services) >= 1
        assert len(result.calls) >= 1

    def test_extract_all_filters_non_go_files(self):
        extractor = GoASTExtractor()
        files = [
            {"path": "app.py", "content": PYTHON_CODE},
            {"path": "main.go", "content": GO_HTTP_SERVER},
        ]
        result = extractor.extract_all(files)
        services = [s for s in result.services if s.language == "go"]
        assert len(services) >= 1


class TestASTExtractionProducesScopedIds:

    def test_ast_extraction_produces_scoped_ids(self) -> None:
        files = [
            {"path": "services/auth/main.go", "content": (
                "package main\n"
                "import \"net/http\"\n"
                "func main() {\n"
                "  http.ListenAndServe(\":8080\", nil)\n"
                "}\n"
            )},
        ]
        extractor = GoASTExtractor()
        result = extractor.extract_all(files)
        if result.services:
            svc_id = result.services[0].service_id
            assert "::" in svc_id, (
                f"Service ID '{svc_id}' is not scoped. Expected "
                "'repository::namespace::name' format from ScopedEntityId."
            )
