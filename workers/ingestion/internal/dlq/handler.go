package dlq

import (
	"context"
	"log/slog"

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

type Handler struct {
	source   <-chan domain.Result
	sink     DeadLetterSink
	observer SinkErrorObserver
	logger   *slog.Logger
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

func NewHandler(source <-chan domain.Result, sink DeadLetterSink, opts ...Option) *Handler {
	h := &Handler{
		source:   source,
		sink:     sink,
		observer: noopSinkObserver{},
		logger:   slog.Default(),
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
			if err := h.sink.Send(ctx, result); err != nil {
				h.logger.Error("dlq sink publish failed",
					"error", err,
					"job_key", string(result.Job.Key),
					"topic", result.Job.Topic,
					"partition", result.Job.Partition,
					"offset", result.Job.Offset,
					"attempts", result.Attempts,
				)
				h.observer.RecordDLQSinkError()
			}
		}
	}
}
