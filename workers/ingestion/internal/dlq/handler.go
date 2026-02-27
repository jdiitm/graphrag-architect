package dlq

import (
	"context"
	"log/slog"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

type DeadLetterSink interface {
	Send(ctx context.Context, result domain.Result) error
}

type SinkErrorObserver interface {
	RecordDLQSinkError()
}

type noopSinkObserver struct{}

func (noopSinkObserver) RecordDLQSinkError() {}

type Config struct {
	MaxSinkRetries int
	RetryDelay     time.Duration
}

type Handler struct {
	source   <-chan domain.Result
	sink     DeadLetterSink
	fallback DeadLetterSink
	observer SinkErrorObserver
	logger   *slog.Logger
	cfg      Config
}

type Option func(*Handler)

func WithObserver(obs SinkErrorObserver) Option {
	return func(h *Handler) {
		h.observer = obs
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(h *Handler) {
		h.logger = logger
	}
}

func WithConfig(cfg Config) Option {
	return func(h *Handler) {
		h.cfg = cfg
	}
}

func WithFallback(fallback DeadLetterSink) Option {
	return func(h *Handler) {
		h.fallback = fallback
	}
}

func NewHandler(source <-chan domain.Result, sink DeadLetterSink, opts ...Option) *Handler {
	h := &Handler{
		source:   source,
		sink:     sink,
		observer: noopSinkObserver{},
		logger:   slog.Default(),
		cfg:      Config{MaxSinkRetries: 3, RetryDelay: 500 * time.Millisecond},
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

func (h *Handler) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case result, ok := <-h.source:
			if !ok {
				return
			}
			h.processResult(ctx, result)
		}
	}
}

func (h *Handler) processResult(ctx context.Context, result domain.Result) {
	defer h.closeDone(result)

	var lastErr error
	attempts := 1 + h.cfg.MaxSinkRetries
	for i := 0; i < attempts; i++ {
		if i > 0 && h.cfg.RetryDelay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(h.cfg.RetryDelay):
			}
		}
		lastErr = h.sink.Send(ctx, result)
		if lastErr == nil {
			return
		}
	}

	h.logger.Error("dlq sink publish failed",
		"error", lastErr,
		"job_key", string(result.Job.Key),
		"topic", result.Job.Topic,
		"partition", result.Job.Partition,
		"offset", result.Job.Offset,
		"attempts", result.Attempts,
		"sink_retries", attempts,
	)
	h.observer.RecordDLQSinkError()

	if h.fallback != nil {
		if err := h.fallback.Send(ctx, result); err != nil {
			h.logger.Error("dlq fallback write failed",
				"error", err,
				"job_key", string(result.Job.Key),
			)
		}
	}
}

func (h *Handler) closeDone(result domain.Result) {
	if result.Done != nil {
		close(result.Done)
	}
}
