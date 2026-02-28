package consumer_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/consumer"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

type inflightTrackingSource struct {
	batches   [][]domain.Job
	index     int
	pollCount atomic.Int32
	mu        sync.Mutex
}

func (s *inflightTrackingSource) Poll(_ context.Context) ([]domain.Job, error) {
	if int(s.pollCount.Add(1)-1) >= len(s.batches) {
		return nil, consumer.ErrSourceClosed
	}
	s.mu.Lock()
	idx := s.index
	s.index++
	s.mu.Unlock()
	if idx >= len(s.batches) {
		return nil, consumer.ErrSourceClosed
	}
	return s.batches[idx], nil
}

func (s *inflightTrackingSource) Commit(_ context.Context) error { return nil }
func (s *inflightTrackingSource) Close()                        {}

func TestConsumer_MaxInflightLimitsConcurrentJobs(t *testing.T) {
	batchSize := 5
	batches := make([][]domain.Job, 3)
	for i := range batches {
		batch := make([]domain.Job, batchSize)
		for j := range batch {
			batch[j] = sampleJob("job-" + string(rune('0'+i)) + "-" + string(rune('0'+j)))
		}
		batches[i] = batch
	}

	src := &inflightTrackingSource{batches: batches}
	jobs := make(chan domain.Job, 20)
	acks := make(chan struct{}, 20)

	maxInflight := 3
	c := consumer.New(src, jobs, acks,
		consumer.WithMaxInflight(maxInflight),
	)

	var peakInflight atomic.Int32
	var currentInflight atomic.Int32

	go func() {
		for j := range jobs {
			_ = j
			cur := currentInflight.Add(1)
			for {
				peak := peakInflight.Load()
				if cur <= peak || peakInflight.CompareAndSwap(peak, cur) {
					break
				}
			}

			time.Sleep(10 * time.Millisecond)
			currentInflight.Add(-1)
			acks <- struct{}{}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := c.Run(ctx)
	close(jobs)

	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("unexpected error: %v", err)
	}

	peak := int(peakInflight.Load())
	if peak > maxInflight {
		t.Fatalf(
			"peak inflight was %d, but maxInflight was set to %d — "+
				"consumer must respect backpressure",
			peak, maxInflight,
		)
	}
}

func TestConsumer_BackpressureWaitExitsOnBatchTimeout(t *testing.T) {
	src := &inflightTrackingSource{
		batches: [][]domain.Job{
			{sampleJob("j1"), sampleJob("j2")},
		},
	}
	jobs := make(chan domain.Job, 10)
	acks := make(chan struct{}, 10)

	c := consumer.New(src, jobs, acks,
		consumer.WithMaxInflight(1),
		consumer.WithMaxBatchWait(50*time.Millisecond),
	)

	go func() {
		for range jobs {
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- c.Run(context.Background())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal(
			"consumer did not complete — spin-loop in backpressure " +
				"wait when maxInflight and maxBatchWait are combined",
		)
	}
}

func TestConsumer_DefaultMaxInflightIsZeroMeansUnlimited(t *testing.T) {
	src := &stubSource{
		batches: [][]domain.Job{
			{sampleJob("a"), sampleJob("b")},
		},
	}
	jobs := make(chan domain.Job, 10)
	acks := make(chan struct{}, 10)

	c := consumer.New(src, jobs, acks)

	go func() {
		for range 2 {
			<-jobs
			acks <- struct{}{}
		}
	}()

	err := c.Run(context.Background())
	if err != nil {
		t.Fatalf("expected nil with default (unlimited) inflight, got %v", err)
	}
}
