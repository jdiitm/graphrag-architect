package consumer

import (
	"context"
	"errors"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

var ErrSourceClosed = errors.New("source closed")

type JobSource interface {
	Poll(ctx context.Context) ([]domain.Job, error)
	Close()
}

type Consumer struct {
	source JobSource
	jobs   chan<- domain.Job
}

func New(source JobSource, jobs chan<- domain.Job) *Consumer {
	return &Consumer{
		source: source,
		jobs:   jobs,
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
	}
}
