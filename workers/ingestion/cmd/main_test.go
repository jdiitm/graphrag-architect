package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/consumer"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dispatcher"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dlq"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/processor"
)

type stubSink struct {
	results []domain.Result
}

func (s *stubSink) Send(_ context.Context, r domain.Result) error {
	s.results = append(s.results, r)
	return nil
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
	var received int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received++
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

	if received != 2 {
		t.Errorf("orchestrator received %d requests, want 2", received)
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

	if len(sink.results) != 1 {
		t.Fatalf("DLQ received %d results, want 1", len(sink.results))
	}
	if sink.results[0].Err == nil {
		t.Error("DLQ result should have error")
	}
}
