package processor_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/processor"
)

func validJob() domain.Job {
	return domain.Job{
		Key:   []byte("repo-hash-abc"),
		Value: []byte("package main\n\nfunc main() {}"),
		Topic: "raw-documents",
		Headers: map[string]string{
			"file_path":   "cmd/main.go",
			"source_type": "source_code",
			"repository":  "graphrag-architect",
			"commit_sha":  "abc123",
		},
	}
}

func TestForwardingProcessor_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"committed","entities_extracted":2,"errors":[]}`))
	}))
	defer srv.Close()

	fp := processor.NewForwardingProcessor(srv.URL, srv.Client())
	err := fp.Process(context.Background(), validJob())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestForwardingProcessor_PostsCorrectJSON(t *testing.T) {
	var captured []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"committed","entities_extracted":0,"errors":[]}`))
	}))
	defer srv.Close()

	job := validJob()
	fp := processor.NewForwardingProcessor(srv.URL, srv.Client())
	_ = fp.Process(context.Background(), job)

	var payload map[string]interface{}
	if err := json.Unmarshal(captured, &payload); err != nil {
		t.Fatalf("request body is not valid JSON: %v", err)
	}

	docs, ok := payload["documents"].([]interface{})
	if !ok || len(docs) != 1 {
		t.Fatalf("expected documents array with 1 element, got %v", payload["documents"])
	}

	doc := docs[0].(map[string]interface{})
	if doc["file_path"] != "cmd/main.go" {
		t.Errorf("file_path = %v, want cmd/main.go", doc["file_path"])
	}
	if doc["source_type"] != "source_code" {
		t.Errorf("source_type = %v, want source_code", doc["source_type"])
	}
	if doc["repository"] != "graphrag-architect" {
		t.Errorf("repository = %v, want graphrag-architect", doc["repository"])
	}
	if doc["commit_sha"] != "abc123" {
		t.Errorf("commit_sha = %v, want abc123", doc["commit_sha"])
	}
}

func TestForwardingProcessor_Base64EncodesContent(t *testing.T) {
	var captured []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"committed","entities_extracted":0,"errors":[]}`))
	}))
	defer srv.Close()

	raw := []byte("apiVersion: apps/v1\nkind: Deployment")
	job := validJob()
	job.Value = raw

	fp := processor.NewForwardingProcessor(srv.URL, srv.Client())
	_ = fp.Process(context.Background(), job)

	var payload struct {
		Documents []struct {
			Content string `json:"content"`
		} `json:"documents"`
	}
	if err := json.Unmarshal(captured, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	want := base64.StdEncoding.EncodeToString(raw)
	if payload.Documents[0].Content != want {
		t.Errorf("content = %q, want base64 %q", payload.Documents[0].Content, want)
	}
}

func TestForwardingProcessor_PostsToIngestEndpoint(t *testing.T) {
	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"committed","entities_extracted":0,"errors":[]}`))
	}))
	defer srv.Close()

	fp := processor.NewForwardingProcessor(srv.URL, srv.Client())
	_ = fp.Process(context.Background(), validJob())

	if capturedPath != "/ingest" {
		t.Errorf("POST path = %q, want /ingest", capturedPath)
	}
}

func TestForwardingProcessor_SetsContentTypeJSON(t *testing.T) {
	var capturedContentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedContentType = r.Header.Get("Content-Type")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"committed","entities_extracted":0,"errors":[]}`))
	}))
	defer srv.Close()

	fp := processor.NewForwardingProcessor(srv.URL, srv.Client())
	_ = fp.Process(context.Background(), validJob())

	if capturedContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", capturedContentType)
	}
}

func TestForwardingProcessor_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	fp := processor.NewForwardingProcessor(srv.URL, srv.Client())
	err := fp.Process(context.Background(), validJob())
	if err == nil {
		t.Fatal("expected error for 500 response, got nil")
	}
}

func TestForwardingProcessor_ConnectionRefused(t *testing.T) {
	fp := processor.NewForwardingProcessor("http://127.0.0.1:1", http.DefaultClient)
	err := fp.Process(context.Background(), validJob())
	if err == nil {
		t.Fatal("expected error for connection refused, got nil")
	}
}

func TestForwardingProcessor_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"committed","entities_extracted":0,"errors":[]}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fp := processor.NewForwardingProcessor(srv.URL, srv.Client())
	err := fp.Process(ctx, validJob())
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestForwardingProcessor_MissingFilePath(t *testing.T) {
	fp := processor.NewForwardingProcessor("http://localhost:8080", http.DefaultClient)
	job := validJob()
	delete(job.Headers, "file_path")

	err := fp.Process(context.Background(), job)
	if err == nil {
		t.Fatal("expected error for missing file_path header, got nil")
	}
}

func TestForwardingProcessor_MissingSourceType(t *testing.T) {
	fp := processor.NewForwardingProcessor("http://localhost:8080", http.DefaultClient)
	job := validJob()
	delete(job.Headers, "source_type")

	err := fp.Process(context.Background(), job)
	if err == nil {
		t.Fatal("expected error for missing source_type header, got nil")
	}
}

func TestForwardingProcessor_OptionalHeadersOmitted(t *testing.T) {
	var captured []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"committed","entities_extracted":0,"errors":[]}`))
	}))
	defer srv.Close()

	job := validJob()
	delete(job.Headers, "repository")
	delete(job.Headers, "commit_sha")

	fp := processor.NewForwardingProcessor(srv.URL, srv.Client())
	err := fp.Process(context.Background(), job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var payload struct {
		Documents []struct {
			Repository *string `json:"repository"`
			CommitSHA  *string `json:"commit_sha"`
		} `json:"documents"`
	}
	if err := json.Unmarshal(captured, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if payload.Documents[0].Repository != nil {
		t.Errorf("repository should be omitted, got %v", *payload.Documents[0].Repository)
	}
	if payload.Documents[0].CommitSHA != nil {
		t.Errorf("commit_sha should be omitted, got %v", *payload.Documents[0].CommitSHA)
	}
}
