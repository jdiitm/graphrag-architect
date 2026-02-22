package dispatcher_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dispatcher"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/telemetry"
)

var errProcessing = errors.New("processing failed")

type spyProcessor struct {
	mu       sync.Mutex
	calls    []domain.Job
	handler  func(ctx context.Context, job domain.Job) error
}

func (s *spyProcessor) Process(ctx context.Context, job domain.Job) error {
	s.mu.Lock()
	s.calls = append(s.calls, job)
	s.mu.Unlock()
	if s.handler != nil {
		return s.handler(ctx, job)
	}
	return nil
}

func (s *spyProcessor) CallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func makeJob(key string) domain.Job {
	return domain.Job{
		Key:       []byte(key),
		Value:     []byte("payload"),
		Topic:     "raw-documents",
		Partition: 0,
		Offset:    1,
		Timestamp: time.Now(),
	}
}

func TestDispatcherProcessesJob(t *testing.T) {
	proc := &spyProcessor{}
	cfg := dispatcher.Config{NumWorkers: 1, MaxRetries: 1, JobBuffer: 1, DLQBuffer: 1}
	d := dispatcher.New(cfg, proc)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()

	job := makeJob("test-key")
	d.Jobs() <- job

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if proc.CallCount() != 1 {
		t.Fatalf("expected Process called 1 time, got %d", proc.CallCount())
	}
}

func TestDispatcherProcessesConcurrently(t *testing.T) {
	const numWorkers = 4
	const numJobs = 4

	var inflight atomic.Int32
	var maxInflight atomic.Int32

	proc := &spyProcessor{
		handler: func(ctx context.Context, job domain.Job) error {
			cur := inflight.Add(1)
			for {
				old := maxInflight.Load()
				if cur <= old || maxInflight.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			inflight.Add(-1)
			return nil
		},
	}

	cfg := dispatcher.Config{NumWorkers: numWorkers, MaxRetries: 1, JobBuffer: numJobs, DLQBuffer: 1}
	d := dispatcher.New(cfg, proc)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()

	for i := 0; i < numJobs; i++ {
		d.Jobs() <- makeJob("concurrent")
	}

	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	if proc.CallCount() != numJobs {
		t.Fatalf("expected %d calls, got %d", numJobs, proc.CallCount())
	}
	if maxInflight.Load() < 2 {
		t.Fatalf("expected concurrent execution (max inflight >= 2), got %d", maxInflight.Load())
	}
}

func TestDispatcherRetriesOnError(t *testing.T) {
	var attempts atomic.Int32

	proc := &spyProcessor{
		handler: func(ctx context.Context, job domain.Job) error {
			if attempts.Add(1) < 3 {
				return errProcessing
			}
			return nil
		},
	}

	cfg := dispatcher.Config{NumWorkers: 1, MaxRetries: 3, JobBuffer: 1, DLQBuffer: 1}
	d := dispatcher.New(cfg, proc)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()

	d.Jobs() <- makeJob("retry-key")

	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}

	select {
	case r := <-d.DLQ():
		t.Fatalf("job should NOT be in DLQ after successful retry, got %+v", r)
	default:
	}
}

func TestDispatcherSendsToDLQAfterMaxRetries(t *testing.T) {
	proc := &spyProcessor{
		handler: func(ctx context.Context, job domain.Job) error {
			return errProcessing
		},
	}

	cfg := dispatcher.Config{NumWorkers: 1, MaxRetries: 2, JobBuffer: 1, DLQBuffer: 1}
	d := dispatcher.New(cfg, proc)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()

	d.Jobs() <- makeJob("dlq-key")

	select {
	case result := <-d.DLQ():
		if result.Err == nil {
			t.Fatal("expected non-nil error in DLQ result")
		}
		if result.Attempts != 2 {
			t.Fatalf("expected 2 attempts, got %d", result.Attempts)
		}
		if string(result.Job.Key) != "dlq-key" {
			t.Fatalf("expected job key 'dlq-key', got '%s'", string(result.Job.Key))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for DLQ result")
	}

	cancel()
	<-done
}

func TestDispatcherGracefulShutdown(t *testing.T) {
	started := make(chan struct{})
	proc := &spyProcessor{
		handler: func(ctx context.Context, job domain.Job) error {
			close(started)
			time.Sleep(200 * time.Millisecond)
			return nil
		},
	}

	cfg := dispatcher.Config{NumWorkers: 1, MaxRetries: 1, JobBuffer: 1, DLQBuffer: 1}
	d := dispatcher.New(cfg, proc)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()

	d.Jobs() <- makeJob("shutdown-key")
	<-started

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after context cancellation and in-flight completion")
	}

	if proc.CallCount() != 1 {
		t.Fatalf("expected in-flight job to complete, got %d calls", proc.CallCount())
	}
}

func TestDispatcherExtractsTraceContextFromJobHeaders(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp, err := telemetry.Init(telemetry.WithExporter(exp))
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, parentSpan := tp.Tracer("test-producer").Start(context.Background(), "kafka.produce")
	parentSC := parentSpan.SpanContext()
	traceparent := fmt.Sprintf("00-%s-%s-01", parentSC.TraceID(), parentSC.SpanID())
	parentSpan.End()

	proc := &spyProcessor{}
	cfg := dispatcher.Config{NumWorkers: 1, MaxRetries: 1, JobBuffer: 1, DLQBuffer: 1}
	d := dispatcher.New(cfg, proc)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()

	job := makeJob("trace-key")
	job.Headers = map[string]string{
		"file_path":   "test.go",
		"source_type": "source_code",
		"traceparent": traceparent,
	}
	d.Jobs() <- job

	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	spans := exp.GetSpans()
	var processSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "job.process" {
			processSpan = &spans[i]
			break
		}
	}
	if processSpan == nil {
		t.Fatal("expected job.process span")
	}
	if processSpan.SpanContext.TraceID() != parentSC.TraceID() {
		t.Errorf("process span trace ID = %s, want %s (should inherit from traceparent)",
			processSpan.SpanContext.TraceID(), parentSC.TraceID())
	}
	if processSpan.Parent.SpanID() == (otelTrace.SpanID{}) {
		t.Error("process span should have a parent span ID from traceparent extraction")
	}
}

func TestDispatcherDLQBlocksAckUntilDoneSignal(t *testing.T) {
	proc := &spyProcessor{
		handler: func(ctx context.Context, job domain.Job) error {
			return errProcessing
		},
	}

	cfg := dispatcher.Config{NumWorkers: 1, MaxRetries: 1, JobBuffer: 1, DLQBuffer: 1}
	d := dispatcher.New(cfg, proc)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()

	d.Jobs() <- makeJob("dlq-block-key")

	var dlqResult domain.Result
	select {
	case dlqResult = <-d.DLQ():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for DLQ result")
	}

	if dlqResult.Done == nil {
		t.Fatal("expected Done channel on DLQ result")
	}

	select {
	case <-d.Acks():
		t.Fatal("ack sent before DLQ Done signal")
	case <-time.After(100 * time.Millisecond):
	}

	close(dlqResult.Done)

	select {
	case <-d.Acks():
	case <-time.After(2 * time.Second):
		t.Fatal("ack not sent after DLQ Done signal")
	}

	cancel()
	<-done
}

func TestDispatcherDLQAckUnblockedByContextCancel(t *testing.T) {
	proc := &spyProcessor{
		handler: func(ctx context.Context, job domain.Job) error {
			return errProcessing
		},
	}

	cfg := dispatcher.Config{NumWorkers: 1, MaxRetries: 1, JobBuffer: 1, DLQBuffer: 1}
	d := dispatcher.New(cfg, proc)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()

	d.Jobs() <- makeJob("dlq-cancel-key")

	select {
	case <-d.DLQ():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for DLQ result")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not exit after context cancel while waiting on DLQ Done")
	}
}

func TestDispatcherDrainsOnClosedChannel(t *testing.T) {
	proc := &spyProcessor{}
	cfg := dispatcher.Config{NumWorkers: 2, MaxRetries: 1, JobBuffer: 4, DLQBuffer: 1}
	d := dispatcher.New(cfg, proc)

	d.Jobs() <- makeJob("drain-1")
	d.Jobs() <- makeJob("drain-2")
	close(d.Jobs())

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after jobs channel closed")
	}

	if proc.CallCount() != 2 {
		t.Fatalf("expected 2 processed jobs, got %d", proc.CallCount())
	}
}
