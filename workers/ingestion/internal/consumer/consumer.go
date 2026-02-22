package consumer

import (
	"context"
	"errors"
	"fmt"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/telemetry"
)

var ErrSourceClosed = errors.New("source closed")

type JobSource interface {
	Poll(ctx context.Context) ([]domain.Job, error)
	Commit(ctx context.Context) error
	Close()
}

type Consumer struct {
	source JobSource
	jobs   chan<- domain.Job
	acks   <-chan struct{}
}

func New(source JobSource, jobs chan<- domain.Job, acks <-chan struct{}) *Consumer {
	return &Consumer{
		source: source,
		jobs:   jobs,
		acks:   acks,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		batch, err := c.source.Poll(ctx)
		if errors.Is(err, ErrSourceClosed) {
			return nil
		}
		if err != nil {
			return err
		}

		pollCtx, pollSpan := telemetry.StartPollSpan(ctx, len(batch))

		for _, job := range batch {
			select {
			case <-pollCtx.Done():
				pollSpan.End()
				return pollCtx.Err()
			case c.jobs <- job:
			}
		}

		for i := 0; i < len(batch); i++ {
			select {
			case <-pollCtx.Done():
				pollSpan.End()
				return pollCtx.Err()
			case <-c.acks:
			}
		}

		commitCtx, commitSpan := telemetry.StartCommitSpan(pollCtx)
		if err := c.source.Commit(commitCtx); err != nil {
			commitSpan.End()
			pollSpan.End()
			return fmt.Errorf("offset commit: %w", err)
		}
		commitSpan.End()
		pollSpan.End()
	}
}
