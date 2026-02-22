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

type retrySink struct {
	mu        sync.Mutex
	calls     int
	failUntil int
	sendErr   error
}

func (s *retrySink) Send(_ context.Context, result domain.Result) error {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call <= s.failUntil {
		return s.sendErr
	}
	return nil
}

func (s *retrySink) CallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
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
	handler := dlq.NewHandler(source, sink,
		dlq.WithObserver(observer),
		dlq.WithConfig(dlq.Config{MaxSinkRetries: 0}),
	)

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
	handler := dlq.NewHandler(source, sink,
		dlq.WithObserver(observer),
		dlq.WithConfig(dlq.Config{MaxSinkRetries: 0}),
	)

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

func TestDLQHandlerRetriesSinkOnTransientError(t *testing.T) {
	source := make(chan domain.Result, 1)
	sink := &retrySink{failUntil: 2, sendErr: errors.New("transient")}
	handler := dlq.NewHandler(source, sink,
		dlq.WithConfig(dlq.Config{MaxSinkRetries: 3, RetryDelay: 0}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		handler.Run(ctx)
		close(done)
	}()

	resultDone := make(chan struct{})
	source <- domain.Result{
		Job:      domain.Job{Key: []byte("retry-me")},
		Err:      errors.New("fail"),
		Attempts: 1,
		Done:     resultDone,
	}

	select {
	case <-resultDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Done signal")
	}

	cancel()
	<-done

	if sink.CallCount() != 3 {
		t.Fatalf("expected 3 sink attempts (2 failures + 1 success), got %d", sink.CallCount())
	}
}

func TestDLQHandlerClosesDoneOnSuccess(t *testing.T) {
	source := make(chan domain.Result, 1)
	sink := &spySink{}
	handler := dlq.NewHandler(source, sink)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		handler.Run(ctx)
		close(done)
	}()

	resultDone := make(chan struct{})
	source <- domain.Result{
		Job:      domain.Job{Key: []byte("done-test")},
		Err:      errors.New("fail"),
		Attempts: 1,
		Done:     resultDone,
	}

	select {
	case <-resultDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Done channel not closed after successful sink write")
	}

	cancel()
	<-done
}

func TestDLQHandlerDoneNotClosedWhenSinkFailsNoFallback(t *testing.T) {
	source := make(chan domain.Result, 1)
	sink := &spySink{sendErr: errors.New("permanent failure")}
	handler := dlq.NewHandler(source, sink,
		dlq.WithConfig(dlq.Config{MaxSinkRetries: 2, RetryDelay: 0}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	handlerDone := make(chan struct{})
	go func() {
		handler.Run(ctx)
		close(handlerDone)
	}()

	resultDone := make(chan struct{})
	source <- domain.Result{
		Job:      domain.Job{Key: []byte("exhaust-test")},
		Err:      errors.New("fail"),
		Attempts: 1,
		Done:     resultDone,
	}

	time.Sleep(200 * time.Millisecond)

	select {
	case <-resultDone:
		t.Fatal("Done channel must NOT be closed when sink fails and no fallback exists — closing it allows silent message loss via premature offset commit")
	default:
	}

	cancel()
	<-handlerDone
}

func TestDLQHandlerDoneNotClosedWhenFallbackFails(t *testing.T) {
	source := make(chan domain.Result, 1)
	sink := &spySink{sendErr: errors.New("primary failure")}
	fallback := &spySink{sendErr: errors.New("fallback failure")}
	handler := dlq.NewHandler(source, sink,
		dlq.WithConfig(dlq.Config{MaxSinkRetries: 0, RetryDelay: 0}),
		dlq.WithFallback(fallback),
	)

	ctx, cancel := context.WithCancel(context.Background())
	handlerDone := make(chan struct{})
	go func() {
		handler.Run(ctx)
		close(handlerDone)
	}()

	resultDone := make(chan struct{})
	source <- domain.Result{
		Job:      domain.Job{Key: []byte("double-fault")},
		Err:      errors.New("processing failed"),
		Attempts: 3,
		Done:     resultDone,
	}

	time.Sleep(200 * time.Millisecond)

	select {
	case <-resultDone:
		t.Fatal("Done channel must NOT be closed when both sink and fallback fail — closing it causes silent message loss")
	default:
	}

	cancel()
	<-handlerDone
}

func TestDLQHandlerFallbackOnPermanentFailure(t *testing.T) {
	source := make(chan domain.Result, 1)
	sink := &spySink{sendErr: errors.New("permanent failure")}
	fallback := &spySink{}
	handler := dlq.NewHandler(source, sink,
		dlq.WithConfig(dlq.Config{MaxSinkRetries: 1, RetryDelay: 0}),
		dlq.WithFallback(fallback),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		handler.Run(ctx)
		close(done)
	}()

	resultDone := make(chan struct{})
	source <- domain.Result{
		Job:      domain.Job{Key: []byte("fallback-test"), Topic: "raw-documents"},
		Err:      errors.New("fail"),
		Attempts: 3,
		Done:     resultDone,
	}

	select {
	case <-resultDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Done signal")
	}

	cancel()
	<-done

	if fallback.Count() != 1 {
		t.Fatalf("expected 1 fallback write, got %d", fallback.Count())
	}
	if string(fallback.Last().Job.Key) != "fallback-test" {
		t.Fatalf("fallback got wrong job key: %s", string(fallback.Last().Job.Key))
	}
}

func TestDLQHandlerNilDoneChannelIsHandledSafely(t *testing.T) {
	source := make(chan domain.Result, 1)
	sink := &spySink{}
	handler := dlq.NewHandler(source, sink)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		handler.Run(ctx)
		close(done)
	}()

	source <- domain.Result{
		Job:      domain.Job{Key: []byte("nil-done")},
		Err:      errors.New("fail"),
		Attempts: 1,
	}

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if sink.Count() != 1 {
		t.Fatalf("expected 1 sink write, got %d", sink.Count())
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
