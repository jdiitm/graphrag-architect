package dlq_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dlq"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

type spySink struct {
	mu      sync.Mutex
	results []domain.Result
}

func (s *spySink) Send(ctx context.Context, result domain.Result) error {
	s.mu.Lock()
	s.results = append(s.results, result)
	s.mu.Unlock()
	return nil
}

func (s *spySink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.results)
}

func (s *spySink) Last() domain.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.results[len(s.results)-1]
}

func TestDLQHandlerForwardsToSink(t *testing.T) {
	source := make(chan domain.Result, 1)
	sink := &spySink{}
	handler := dlq.NewHandler(source, sink)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		handler.Run(ctx)
		close(done)
	}()

	expected := domain.Result{
		Job: domain.Job{
			Key:   []byte("failed-doc"),
			Value: []byte("content"),
			Topic: "raw-documents",
		},
		Err:      errors.New("extraction timeout"),
		Attempts: 3,
	}
	source <- expected

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if sink.Count() != 1 {
		t.Fatalf("expected 1 result forwarded to sink, got %d", sink.Count())
	}
	got := sink.Last()
	if string(got.Job.Key) != "failed-doc" {
		t.Fatalf("expected job key 'failed-doc', got '%s'", string(got.Job.Key))
	}
	if got.Attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", got.Attempts)
	}
}

func TestDLQHandlerStopsOnClosedChannel(t *testing.T) {
	source := make(chan domain.Result, 1)
	sink := &spySink{}
	handler := dlq.NewHandler(source, sink)

	source <- domain.Result{
		Job:      domain.Job{Key: []byte("last-one")},
		Err:      errors.New("fail"),
		Attempts: 1,
	}
	close(source)

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		handler.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after source channel closed")
	}

	if sink.Count() != 1 {
		t.Fatalf("expected 1 result drained before exit, got %d", sink.Count())
	}
}
