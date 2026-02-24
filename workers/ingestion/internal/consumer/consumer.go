package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/metrics"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/telemetry"
)

var (
	ErrSourceClosed = errors.New("source closed")
	ErrAckTimeout   = errors.New("ack timeout: batch processing stalled, possible rebalance risk")
)

type TopicPartition struct {
	Topic     string
	Partition int32
}

type LagReporter interface {
	HighWaterMarks() map[TopicPartition]int64
}

type JobSource interface {
	Poll(ctx context.Context) ([]domain.Job, error)
	Commit(ctx context.Context) error
	Close()
}

type Consumer struct {
	source     JobSource
	observer   metrics.PipelineObserver
	jobs       chan<- domain.Job
	acks       <-chan struct{}
	ackTimeout time.Duration
}

type ConsumerOption func(*Consumer)

func WithObserver(obs metrics.PipelineObserver) ConsumerOption {
	return func(c *Consumer) {
		c.observer = obs
	}
}

func WithAckTimeout(d time.Duration) ConsumerOption {
	return func(c *Consumer) {
		c.ackTimeout = d
	}
}

func New(source JobSource, jobs chan<- domain.Job, acks <-chan struct{}, opts ...ConsumerOption) *Consumer {
	c := &Consumer{
		source:   source,
		observer: metrics.NoopObserver{},
		jobs:     jobs,
		acks:     acks,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		batchStart := time.Now()

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

		if err := c.awaitAcks(pollCtx, len(batch)); err != nil {
			if errors.Is(err, ErrAckTimeout) {
				c.observer.RecordBatchDuration(time.Since(batchStart).Seconds())
				pollSpan.End()
				continue
			}
			pollSpan.End()
			return err
		}

		commitCtx, commitSpan := telemetry.StartCommitSpan(pollCtx)
		if err := c.source.Commit(commitCtx); err != nil {
			commitSpan.End()
			pollSpan.End()
			return fmt.Errorf("offset commit: %w", err)
		}
		commitSpan.End()
		pollSpan.End()

		c.observer.RecordBatchDuration(time.Since(batchStart).Seconds())
		c.reportLag(batch)
	}
}

func (c *Consumer) awaitAcks(ctx context.Context, count int) error {
	if c.ackTimeout <= 0 {
		for i := 0; i < count; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.acks:
			}
		}
		return nil
	}

	timer := time.NewTimer(c.ackTimeout)
	defer timer.Stop()

	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.acks:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(c.ackTimeout)
		case <-timer.C:
			return ErrAckTimeout
		}
	}
	return nil
}

func (c *Consumer) reportLag(batch []domain.Job) {
	reporter, ok := c.source.(LagReporter)
	if !ok {
		return
	}
	watermarks := reporter.HighWaterMarks()
	type offsetEntry struct {
		offset int64
		seen   bool
	}
	maxOffset := make(map[TopicPartition]offsetEntry)
	for _, job := range batch {
		tp := TopicPartition{Topic: job.Topic, Partition: job.Partition}
		entry := maxOffset[tp]
		if !entry.seen || job.Offset > entry.offset {
			maxOffset[tp] = offsetEntry{offset: job.Offset, seen: true}
		}
	}
	for tp, hwm := range watermarks {
		entry, exists := maxOffset[tp]
		if !exists {
			continue
		}
		offset := entry.offset
		lag := hwm - offset
		if lag < 0 {
			lag = 0
		}
		c.observer.RecordConsumerLag(tp.Topic, tp.Partition, lag)
	}
}
