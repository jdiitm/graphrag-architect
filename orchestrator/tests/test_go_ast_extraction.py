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


class TestGoASTExtractorKafka:
    def test_detects_kafka_consumer_topic(self):
        extractor = GoASTExtractor()
        result = extractor.extract("main.go", GO_KAFKA_CONSUMER)
        assert len(result.topics_consumed) >= 1
        assert "raw-documents" in result.topics_consumed


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
