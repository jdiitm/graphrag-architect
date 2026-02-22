package telemetry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/telemetry"
)

func setupTestTracer(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp, err := telemetry.Init(telemetry.WithExporter(exp))
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return exp
}

func sampleJob(id string) domain.Job {
	return domain.Job{
		Key:       []byte(id),
		Value:     []byte("content-" + id),
		Topic:     "raw-documents",
		Partition: 0,
		Offset:    1,
		Headers: map[string]string{
			"file_path":   "file-" + id + ".go",
			"source_type": "source_code",
		},
		Timestamp: time.Now(),
	}
}

func TestStartPollSpan(t *testing.T) {
	exp := setupTestTracer(t)

	ctx, span := telemetry.StartPollSpan(context.Background(), 3)
	_ = ctx
	span.End()

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name != "kafka.poll" {
		t.Errorf("name = %q, want %q", spans[0].Name, "kafka.poll")
	}
	assertAttr(t, spans[0].Attributes, "batch.size", int64(3))
}

func TestStartProcessSpan(t *testing.T) {
	exp := setupTestTracer(t)

	job := sampleJob("x")
	ctx, span := telemetry.StartProcessSpan(context.Background(), job)
	_ = ctx
	span.End()

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name != "job.process" {
		t.Errorf("name = %q, want %q", spans[0].Name, "job.process")
	}
	assertAttr(t, spans[0].Attributes, "job.topic", "raw-documents")
	assertAttr(t, spans[0].Attributes, "job.partition", int64(0))
}

func TestStartForwardSpan(t *testing.T) {
	exp := setupTestTracer(t)

	job := sampleJob("y")
	ctx, span := telemetry.StartForwardSpan(context.Background(), job)
	_ = ctx
	span.End()

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name != "http.forward" {
		t.Errorf("name = %q, want %q", spans[0].Name, "http.forward")
	}
	assertAttr(t, spans[0].Attributes, "http.target", "/ingest")
}

func TestStartDLQSpan(t *testing.T) {
	exp := setupTestTracer(t)

	result := domain.Result{
		Job:      sampleJob("z"),
		Err:      errors.New("server error"),
		Attempts: 3,
	}
	ctx, span := telemetry.StartDLQSpan(context.Background(), result)
	_ = ctx
	span.End()

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name != "dlq.route" {
		t.Errorf("name = %q, want %q", spans[0].Name, "dlq.route")
	}
	assertAttr(t, spans[0].Attributes, "dlq.attempts", int64(3))
	assertAttr(t, spans[0].Attributes, "dlq.error", "server error")
}

func TestStartCommitSpan(t *testing.T) {
	exp := setupTestTracer(t)

	ctx, span := telemetry.StartCommitSpan(context.Background())
	_ = ctx
	span.End()

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name != "kafka.commit" {
		t.Errorf("name = %q, want %q", spans[0].Name, "kafka.commit")
	}
}

func assertAttr(t *testing.T, attrs []attribute.KeyValue, key string, want interface{}) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			switch v := want.(type) {
			case string:
				if a.Value.AsString() != v {
					t.Errorf("attr %q = %v, want %v", key, a.Value.AsString(), v)
				}
			case int64:
				if a.Value.AsInt64() != v {
					t.Errorf("attr %q = %v, want %v", key, a.Value.AsInt64(), v)
				}
			}
			return
		}
	}
	t.Errorf("attribute %q not found", key)
}
