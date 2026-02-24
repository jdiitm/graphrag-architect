package processor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

type captureProcessor struct {
	job domain.Job
}

func (p *captureProcessor) Process(_ context.Context, job domain.Job) error {
	p.job = job
	return nil
}

func TestASTProcessorPassesThroughNonGo(t *testing.T) {
	downstream := &captureProcessor{}
	proc := NewASTProcessor(downstream)
	payload, _ := json.Marshal(map[string]string{
		"file_path": "main.py",
		"content":   "print('hello')",
	})
	job := domain.Job{
		Key:       []byte("test"),
		Value:     payload,
		Topic:     "test",
		Partition: 0,
		Offset:    1,
		Headers:   map[string]string{},
		Timestamp: time.Now(),
	}
	if err := proc.Process(context.Background(), job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(downstream.job.Value) == 0 {
		t.Fatal("downstream should have received the job")
	}
}

func TestASTProcessorEnrichesGoFile(t *testing.T) {
	downstream := &captureProcessor{}
	proc := NewASTProcessor(downstream)
	goContent := `package main

import "net/http"

func handler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}
`
	payload, _ := json.Marshal(map[string]string{
		"file_path": "server.go",
		"content":   goContent,
	})
	job := domain.Job{
		Key:       []byte("test"),
		Value:     payload,
		Topic:     "test",
		Partition: 0,
		Offset:    1,
		Headers:   map[string]string{},
		Timestamp: time.Now(),
	}
	if err := proc.Process(context.Background(), job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var enriched map[string]interface{}
	if err := json.Unmarshal(downstream.job.Value, &enriched); err != nil {
		t.Fatalf("failed to parse enriched payload: %v", err)
	}
	if _, ok := enriched["ast_result"]; !ok {
		t.Fatal("enriched payload should contain ast_result")
	}
}

func TestExtractGoAST(t *testing.T) {
	src := `package myservice

import (
	"net/http"
	"google.golang.org/grpc"
)

func Start() {
	http.Get("http://auth-service:8080/verify")
}
`
	result, err := ExtractGoAST("myservice/main.go", src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.PackageName != "myservice" {
		t.Errorf("expected package 'myservice', got %q", result.PackageName)
	}
	if len(result.Imports) != 2 {
		t.Errorf("expected 2 imports, got %d", len(result.Imports))
	}
	if len(result.ServiceHints) != 2 {
		t.Errorf("expected 2 service hints (http-server, grpc-server), got %d", len(result.ServiceHints))
	}
	if len(result.Functions) != 1 {
		t.Errorf("expected 1 function, got %d", len(result.Functions))
	}
	if len(result.HTTPCalls) != 1 {
		t.Errorf("expected 1 HTTP call, got %d", len(result.HTTPCalls))
	}
}

func TestExtractGoASTInvalidSyntax(t *testing.T) {
	_, err := ExtractGoAST("bad.go", "not valid go")
	if err == nil {
		t.Fatal("expected error for invalid Go syntax")
	}
}
