package consumer

import (
	"context"
	"errors"
	"fmt"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
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

		for _, job := range batch {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case c.jobs <- job:
			}
		}

		for i := 0; i < len(batch); i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.acks:
			}
		}

		if err := c.source.Commit(ctx); err != nil {
			return fmt.Errorf("offset commit: %w", err)
		}
	}
}
