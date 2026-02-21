package dlq

import (
	"context"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

type DeadLetterSink interface {
	Send(ctx context.Context, result domain.Result) error
}

type Handler struct {
	source <-chan domain.Result
	sink   DeadLetterSink
}

func NewHandler(source <-chan domain.Result, sink DeadLetterSink) *Handler {
	return &Handler{
		source: source,
		sink:   sink,
	}
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
			_ = h.sink.Send(ctx, result)
		}
	}
}
