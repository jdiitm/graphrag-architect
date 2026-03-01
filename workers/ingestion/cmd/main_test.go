package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/consumer"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dispatcher"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dlq"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/processor"
)

type stubSink struct {
	mu      sync.Mutex
	results []domain.Result
}

func (s *stubSink) Send(_ context.Context, r domain.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, r)
	return nil
}

func (s *stubSink) Results() []domain.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]domain.Result, len(s.results))
	copy(cp, s.results)
	return cp
}

type stubJobSource struct {
	batches [][]domain.Job
	index   int
}

func (s *stubJobSource) Poll(_ context.Context) ([]domain.Job, error) {
	if s.index >= len(s.batches) {
		return nil, consumer.ErrSourceClosed
	}
	batch := s.batches[s.index]
	s.index++
	return batch, nil
}

func (s *stubJobSource) Commit(_ context.Context) error { return nil }

func (s *stubJobSource) Close() {}

func TestEndToEnd_ConsumerDispatcherForwarding(t *testing.T) {
	var received atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"committed","entities_extracted":1,"errors":[]}`))
	}))
	defer srv.Close()

	fp := processor.NewForwardingProcessor(srv.URL, srv.Client())

	cfg := dispatcher.Config{
		NumWorkers: 2,
		MaxRetries: 1,
		JobBuffer:  10,
		DLQBuffer:  10,
	}
	disp := dispatcher.New(cfg, fp)

	sink := &stubSink{}
	dlqHandler := dlq.NewHandler(disp.DLQ(), sink)

	source := &stubJobSource{
		batches: [][]domain.Job{
			{
				{
					Key:   []byte("repo1"),
					Value: []byte("package main"),
					Topic: "raw-documents",
					Headers: map[string]string{
						"file_path":   "main.go",
						"source_type": "source_code",
					},
					Timestamp: time.Now(),
				},
				{
					Key:   []byte("repo1"),
					Value: []byte("apiVersion: v1"),
					Topic: "raw-documents",
					Headers: map[string]string{
						"file_path":   "deploy.yaml",
						"source_type": "k8s_manifest",
					},
					Timestamp: time.Now(),
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cons := consumer.New(source, disp.Jobs(), disp.Acks())

	go disp.Run(ctx)
	go dlqHandler.Run(ctx)

	if err := cons.Run(ctx); err != nil {
		t.Fatalf("consumer error: %v", err)
	}

	time.Sleep(500 * time.Millisecond)
	cancel()

	if got := received.Load(); got != 2 {
		t.Errorf("orchestrator received %d requests, want 2", got)
	}
}

func TestEndToEnd_FailedJobRoutesToDLQ(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "fail", http.StatusInternalServerError)
	}))
	defer srv.Close()

	fp := processor.NewForwardingProcessor(srv.URL, srv.Client())

	cfg := dispatcher.Config{
		NumWorkers: 1,
		MaxRetries: 2,
		JobBuffer:  10,
		DLQBuffer:  10,
	}
	disp := dispatcher.New(cfg, fp)

	sink := &stubSink{}
	dlqHandler := dlq.NewHandler(disp.DLQ(), sink)

	source := &stubJobSource{
		batches: [][]domain.Job{
			{
				{
					Key:   []byte("repo1"),
					Value: []byte("bad data"),
					Topic: "raw-documents",
					Headers: map[string]string{
						"file_path":   "bad.go",
						"source_type": "source_code",
					},
					Timestamp: time.Now(),
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cons := consumer.New(source, disp.Jobs(), disp.Acks())

	go disp.Run(ctx)
	go dlqHandler.Run(ctx)

	if err := cons.Run(ctx); err != nil {
		t.Fatalf("consumer error: %v", err)
	}

	time.Sleep(500 * time.Millisecond)
	cancel()

	results := sink.Results()
	if len(results) != 1 {
		t.Fatalf("DLQ received %d results, want 1", len(results))
	}
	if results[0].Err == nil {
		t.Error("DLQ result should have error")
	}
}

func TestValidateBlobStoreForProduction_MemoryInProdReturnsError(t *testing.T) {
	err := validateBlobStoreForProduction("production", "memory")
	if err == nil {
		t.Fatal("expected error when blob store is memory in production mode")
	}
}

func TestValidateBlobStoreForProduction_S3InProdSucceeds(t *testing.T) {
	err := validateBlobStoreForProduction("production", "s3")
	if err != nil {
		t.Fatalf("unexpected error for s3 in production: %v", err)
	}
}

func TestValidateBlobStoreForProduction_MemoryInDevSucceeds(t *testing.T) {
	err := validateBlobStoreForProduction("development", "memory")
	if err != nil {
		t.Fatalf("unexpected error for memory in development: %v", err)
	}
}

func TestValidateBlobStoreForProduction_EmptyModeSucceeds(t *testing.T) {
	err := validateBlobStoreForProduction("", "memory")
	if err != nil {
		t.Fatalf("unexpected error for memory with empty deployment mode: %v", err)
	}
}

func TestValidateDedupStoreForProduction_NoopInProdReturnsError(t *testing.T) {
	err := validateDedupStoreForProduction("production", "noop")
	if err == nil {
		t.Fatal("expected error when dedup store is noop in production mode")
	}
}

func TestValidateDedupStoreForProduction_RedisInProdSucceeds(t *testing.T) {
	err := validateDedupStoreForProduction("production", "redis")
	if err != nil {
		t.Fatalf("unexpected error for redis in production: %v", err)
	}
}

func TestValidateDedupStoreForProduction_MemoryInProdSucceeds(t *testing.T) {
	err := validateDedupStoreForProduction("production", "memory")
	if err != nil {
		t.Fatalf("unexpected error for memory dedup in production: %v", err)
	}
}

func TestValidateDedupStoreForProduction_NoopInDevSucceeds(t *testing.T) {
	err := validateDedupStoreForProduction("development", "noop")
	if err != nil {
		t.Fatalf("unexpected error for noop in development: %v", err)
	}
}

func TestValidateInflightForProduction_ZeroInProdReturnsError(t *testing.T) {
	err := validateInflightForProduction("production", 0)
	if err == nil {
		t.Fatal("expected error when maxInflight is 0 in production mode")
	}
}

func TestValidateInflightForProduction_PositiveInProdSucceeds(t *testing.T) {
	err := validateInflightForProduction("production", 100)
	if err != nil {
		t.Fatalf("unexpected error for maxInflight=100 in production: %v", err)
	}
}

func TestValidateInflightForProduction_ZeroInDevSucceeds(t *testing.T) {
	err := validateInflightForProduction("", 0)
	if err != nil {
		t.Fatalf("unexpected error for maxInflight=0 in dev: %v", err)
	}
}

func TestValidateInflightForProduction_NegativeInProdReturnsError(t *testing.T) {
	err := validateInflightForProduction("production", -1)
	if err == nil {
		t.Fatal("expected error when maxInflight is negative in production mode")
	}
}

func TestValidateDLQFallbackForProduction_PathInProdReturnsError(t *testing.T) {
	err := validateDLQFallbackForProduction("production", "/var/dlq/fallback.jsonl")
	if err == nil {
		t.Fatal("expected error when DLQ fallback path is set in production mode")
	}
}

func TestValidateDLQFallbackForProduction_EmptyPathInProdSucceeds(t *testing.T) {
	err := validateDLQFallbackForProduction("production", "")
	if err != nil {
		t.Fatalf("unexpected error for empty fallback path in production: %v", err)
	}
}

func TestValidateDLQFallbackForProduction_PathInDevSucceeds(t *testing.T) {
	err := validateDLQFallbackForProduction("", "/tmp/dlq.jsonl")
	if err != nil {
		t.Fatalf("unexpected error for fallback path in dev mode: %v", err)
	}
}
