package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

const serviceName = "graphrag-ingestion-worker"

var tracer trace.Tracer

type Option func(*config)

type config struct {
	exporter sdktrace.SpanExporter
	useSync  bool
}

func WithTestExporter() Option {
	return func(c *config) {
		c.exporter = noopExporter{}
		c.useSync = true
	}
}

func WithExporter(exp sdktrace.SpanExporter) Option {
	return func(c *config) {
		c.exporter = exp
		c.useSync = true
	}
}

func Init(opts ...Option) (*sdktrace.TracerProvider, error) {
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}

	if cfg.exporter == nil {
		endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		if endpoint == "" {
			endpoint = "localhost:4317"
		}

		grpcOpts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(endpoint),
		}
		if strings.EqualFold(os.Getenv("OTEL_EXPORTER_OTLP_INSECURE"), "true") {
			grpcOpts = append(grpcOpts, otlptracegrpc.WithInsecure())
		}

		exp, err := otlptracegrpc.New(context.Background(), grpcOpts...)
		if err != nil {
			return nil, fmt.Errorf("create OTLP exporter: %w", err)
		}
		cfg.exporter = exp
	}

	var spanProcessorOpt sdktrace.TracerProviderOption
	if cfg.useSync {
		spanProcessorOpt = sdktrace.WithSyncer(cfg.exporter)
	} else {
		spanProcessorOpt = sdktrace.WithBatcher(cfg.exporter)
	}

	tp := sdktrace.NewTracerProvider(spanProcessorOpt)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tracer = tp.Tracer(serviceName)
	return tp, nil
}

func Tracer() trace.Tracer {
	if tracer == nil {
		return otel.Tracer(serviceName)
	}
	return tracer
}

func StartPollSpan(ctx context.Context, batchSize int) (context.Context, trace.Span) {
	return Tracer().Start(ctx, "kafka.poll",
		trace.WithAttributes(
			attribute.Int64("batch.size", int64(batchSize)),
		),
	)
}

func StartProcessSpan(ctx context.Context, job domain.Job) (context.Context, trace.Span) {
	return Tracer().Start(ctx, "job.process",
		trace.WithAttributes(
			attribute.String("job.topic", job.Topic),
			attribute.Int64("job.partition", int64(job.Partition)),
			attribute.Int64("job.offset", job.Offset),
		),
	)
}

func StartForwardSpan(ctx context.Context, job domain.Job) (context.Context, trace.Span) {
	return Tracer().Start(ctx, "http.forward",
		trace.WithAttributes(
			attribute.String("http.target", "/ingest"),
			attribute.String("job.file_path", job.Headers["file_path"]),
		),
	)
}

func StartStagingSpan(ctx context.Context, job domain.Job, stagingPath string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, "staging.write_and_emit",
		trace.WithAttributes(
			attribute.String("staging.file_path", job.Headers["file_path"]),
			attribute.String("staging.destination", stagingPath),
		),
	)
}

func StartDLQSpan(ctx context.Context, result domain.Result) (context.Context, trace.Span) {
	errMsg := ""
	if result.Err != nil {
		errMsg = result.Err.Error()
	}
	return Tracer().Start(ctx, "dlq.route",
		trace.WithAttributes(
			attribute.Int64("dlq.attempts", int64(result.Attempts)),
			attribute.String("dlq.error", errMsg),
			attribute.String("dlq.topic", result.Job.Topic),
		),
	)
}

func StartCommitSpan(ctx context.Context) (context.Context, trace.Span) {
	return Tracer().Start(ctx, "kafka.commit")
}

func ExtractTraceContext(ctx context.Context, headers map[string]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(headers))
}

func InjectTraceContext(ctx context.Context, headers map[string][]string) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(headers))
}

type noopExporter struct{}

func (noopExporter) ExportSpans(_ context.Context, _ []sdktrace.ReadOnlySpan) error {
	return nil
}

func (noopExporter) Shutdown(_ context.Context) error { return nil }
