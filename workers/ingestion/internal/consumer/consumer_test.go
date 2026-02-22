package consumer_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/consumer"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

type stubSource struct {
	batches [][]domain.Job
	err     error
	index   int
}

func (s *stubSource) Poll(_ context.Context) ([]domain.Job, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.index >= len(s.batches) {
		return nil, consumer.ErrSourceClosed
	}
	batch := s.batches[s.index]
	s.index++
	return batch, nil
}

func (s *stubSource) Close() {}

func sampleJob(id string) domain.Job {
	return domain.Job{
		Key:   []byte(id),
		Value: []byte("content-" + id),
		Topic: "raw-documents",
		Headers: map[string]string{
			"file_path":   "file-" + id + ".go",
			"source_type": "source_code",
		},
		Timestamp: time.Now(),
	}
}

func TestConsumer_PollsAndSendsAllJobs(t *testing.T) {
	src := &stubSource{
		batches: [][]domain.Job{
			{sampleJob("a"), sampleJob("b")},
			{sampleJob("c")},
		},
	}
	jobs := make(chan domain.Job, 10)

	c := consumer.New(src, jobs)
	err := c.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	close(jobs)
	var received []domain.Job
	for j := range jobs {
		received = append(received, j)
	}

	if len(received) != 3 {
		t.Fatalf("received %d jobs, want 3", len(received))
	}
}

func TestConsumer_StopsOnContextCancel(t *testing.T) {
	blocking := &stubSource{
		batches: [][]domain.Job{
			{sampleJob("a")},
			{sampleJob("b")},
			{sampleJob("c")},
			{sampleJob("d")},
			{sampleJob("e")},
		},
	}
	jobs := make(chan domain.Job, 10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := consumer.New(blocking, jobs)
	err := c.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected nil or context.Canceled, got %v", err)
	}
}

func TestConsumer_ReturnsSourceError(t *testing.T) {
	errKafka := errors.New("kafka broker unavailable")
	src := &stubSource{err: errKafka}
	jobs := make(chan domain.Job, 10)

	c := consumer.New(src, jobs)
	err := c.Run(context.Background())
	if !errors.Is(err, errKafka) {
		t.Fatalf("expected %v, got %v", errKafka, err)
	}
}

func TestConsumer_StopsOnSourceClosed(t *testing.T) {
	src := &stubSource{batches: [][]domain.Job{}}
	jobs := make(chan domain.Job, 10)

	c := consumer.New(src, jobs)
	err := c.Run(context.Background())
	if err != nil {
		t.Fatalf("expected nil on clean close, got %v", err)
	}
}
