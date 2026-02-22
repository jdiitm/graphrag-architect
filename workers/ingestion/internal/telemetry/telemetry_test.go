package telemetry_test

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/telemetry"
)

func TestInitReturnsProvider(t *testing.T) {
	tp, err := telemetry.Init(telemetry.WithTestExporter())
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer func() { _ = tp.Shutdown(context.Background()) }()
	if tp == nil {
		t.Fatal("Init() returned nil provider")
	}
}

func TestTracerReturnsNonNil(t *testing.T) {
	tp, _ := telemetry.Init(telemetry.WithTestExporter())
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := telemetry.Tracer()
	if tracer == nil {
		t.Fatal("Tracer() returned nil")
	}
}

func TestTestExporterCapturesSpans(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp, _ := telemetry.Init(telemetry.WithExporter(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := telemetry.Tracer()
	_, span := tracer.Start(context.Background(), "test.span")
	span.End()

	tp.ForceFlush(context.Background())
	spans := exp.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least 1 span, got 0")
	}
	if spans[0].Name != "test.span" {
		t.Errorf("span name = %q, want %q", spans[0].Name, "test.span")
	}
}

func TestWithExporterOption(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp, err := telemetry.Init(telemetry.WithExporter(exp))
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer func() { _ = tp.Shutdown(context.Background()) }()
	if _, ok := interface{}(tp).(*sdktrace.TracerProvider); !ok {
		t.Fatal("expected *sdktrace.TracerProvider")
	}
}
