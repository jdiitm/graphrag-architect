package dlq_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dlq"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

type spySink struct {
	mu      sync.Mutex
	results []domain.Result
	sendErr error
}

func (s *spySink) Send(_ context.Context, result domain.Result) error {
	s.mu.Lock()
	s.results = append(s.results, result)
	s.mu.Unlock()
	return s.sendErr
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

type spyObserver struct {
	mu             sync.Mutex
	sinkErrorCount int
}

func (o *spyObserver) RecordDLQSinkError() {
	o.mu.Lock()
	o.sinkErrorCount++
	o.mu.Unlock()
}

func (o *spyObserver) SinkErrorCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.sinkErrorCount
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

func TestDLQHandlerRecordsMetricOnSinkError(t *testing.T) {
	source := make(chan domain.Result, 1)
	sink := &spySink{sendErr: errors.New("kafka broker unavailable")}
	observer := &spyObserver{}
	handler := dlq.NewHandler(source, sink, dlq.WithObserver(observer))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		handler.Run(ctx)
		close(done)
	}()

	source <- domain.Result{
		Job:      domain.Job{Key: []byte("doomed"), Topic: "raw-documents", Partition: 0, Offset: 42},
		Err:      errors.New("processing failed"),
		Attempts: 3,
	}

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if observer.SinkErrorCount() != 1 {
		t.Fatalf("expected 1 DLQ sink error recorded, got %d", observer.SinkErrorCount())
	}
}

func TestDLQHandlerContinuesAfterSinkError(t *testing.T) {
	source := make(chan domain.Result, 3)
	sink := &spySink{sendErr: errors.New("transient failure")}
	observer := &spyObserver{}
	handler := dlq.NewHandler(source, sink, dlq.WithObserver(observer))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		handler.Run(ctx)
		close(done)
	}()

	for i := 0; i < 3; i++ {
		source <- domain.Result{
			Job:      domain.Job{Key: []byte("msg"), Topic: "raw-documents"},
			Err:      errors.New("fail"),
			Attempts: 1,
		}
	}

	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	if sink.Count() != 3 {
		t.Fatalf("expected 3 Send attempts (handler must continue), got %d", sink.Count())
	}
	if observer.SinkErrorCount() != 3 {
		t.Fatalf("expected 3 DLQ sink errors recorded, got %d", observer.SinkErrorCount())
	}
}

func TestDLQHandlerNoMetricOnSinkSuccess(t *testing.T) {
	source := make(chan domain.Result, 1)
	sink := &spySink{}
	observer := &spyObserver{}
	handler := dlq.NewHandler(source, sink, dlq.WithObserver(observer))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		handler.Run(ctx)
		close(done)
	}()

	source <- domain.Result{
		Job:      domain.Job{Key: []byte("ok")},
		Err:      errors.New("processing err"),
		Attempts: 2,
	}

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if observer.SinkErrorCount() != 0 {
		t.Fatalf("expected 0 DLQ sink errors on success, got %d", observer.SinkErrorCount())
	}
}

func TestDLQHandlerAcceptsLoggerOption(t *testing.T) {
	source := make(chan domain.Result, 1)
	sink := &spySink{sendErr: errors.New("broken")}
	logger := slog.Default()
	handler := dlq.NewHandler(source, sink, dlq.WithLogger(logger))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		handler.Run(ctx)
		close(done)
	}()

	source <- domain.Result{
		Job:      domain.Job{Key: []byte("test"), Topic: "raw-documents"},
		Err:      errors.New("fail"),
		Attempts: 1,
	}

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done
}
