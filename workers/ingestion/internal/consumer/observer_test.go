package consumer_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/consumer"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

type recordingObserver struct {
	mu             sync.Mutex
	batchDurations []float64
	lagRecords     []lagRecord
	jobOutcomes    []string
	dlqCount       int
}

type lagRecord struct {
	topic     string
	partition int32
	lag       int64
}

func (r *recordingObserver) RecordBatchDuration(seconds float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batchDurations = append(r.batchDurations, seconds)
}

func (r *recordingObserver) RecordConsumerLag(topic string, partition int32, lag int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lagRecords = append(r.lagRecords, lagRecord{topic, partition, lag})
}

func (r *recordingObserver) RecordJobProcessed(outcome string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobOutcomes = append(r.jobOutcomes, outcome)
}

func (r *recordingObserver) RecordDLQRouted() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dlqCount++
}

type lagAwareSource struct {
	batches [][]domain.Job
	index   int
}

func (s *lagAwareSource) Poll(_ context.Context) ([]domain.Job, error) {
	if s.index >= len(s.batches) {
		return nil, consumer.ErrSourceClosed
	}
	batch := s.batches[s.index]
	s.index++
	return batch, nil
}

func (s *lagAwareSource) Commit(_ context.Context) error { return nil }
func (s *lagAwareSource) Close()                         {}
func (s *lagAwareSource) HighWaterMarks() map[consumer.TopicPartition]int64 {
	return map[consumer.TopicPartition]int64{
		{Topic: "raw-documents", Partition: 0}: 100,
	}
}

func TestConsumer_RecordsBatchDuration(t *testing.T) {
	src := &lagAwareSource{
		batches: [][]domain.Job{
			{sampleJob("a"), sampleJob("b")},
		},
	}
	obs := &recordingObserver{}
	jobs := make(chan domain.Job, 10)
	acks := make(chan struct{}, 10)

	c := consumer.New(src, jobs, acks, consumer.WithObserver(obs))

	go func() {
		for j := range jobs {
			_ = j
			acks <- struct{}{}
		}
	}()

	err := c.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	close(jobs)

	obs.mu.Lock()
	defer obs.mu.Unlock()

	if len(obs.batchDurations) != 1 {
		t.Fatalf("expected 1 batch duration record, got %d", len(obs.batchDurations))
	}
	if obs.batchDurations[0] <= 0 {
		t.Fatalf("batch duration should be positive, got %f", obs.batchDurations[0])
	}
}

func TestConsumer_RecordsConsumerLag(t *testing.T) {
	src := &lagAwareSource{
		batches: [][]domain.Job{
			{
				{
					Key: []byte("a"), Value: []byte("v"),
					Topic: "raw-documents", Partition: 0, Offset: 90,
					Headers: map[string]string{"file_path": "f.go", "source_type": "source_code"},
					Timestamp: time.Now(),
				},
			},
		},
	}
	obs := &recordingObserver{}
	jobs := make(chan domain.Job, 10)
	acks := make(chan struct{}, 10)

	c := consumer.New(src, jobs, acks, consumer.WithObserver(obs))

	go func() {
		for j := range jobs {
			_ = j
			acks <- struct{}{}
		}
	}()

	err := c.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	close(jobs)

	obs.mu.Lock()
	defer obs.mu.Unlock()

	if len(obs.lagRecords) != 1 {
		t.Fatalf("expected 1 lag record, got %d", len(obs.lagRecords))
	}
	rec := obs.lagRecords[0]
	if rec.topic != "raw-documents" {
		t.Fatalf("expected topic raw-documents, got %s", rec.topic)
	}
	if rec.partition != 0 {
		t.Fatalf("expected partition 0, got %d", rec.partition)
	}
	expectedLag := int64(100 - 90)
	if rec.lag != expectedLag {
		t.Fatalf("expected lag %d, got %d", expectedLag, rec.lag)
	}
}

func TestConsumer_WithoutObserver_StillWorks(t *testing.T) {
	src := &lagAwareSource{
		batches: [][]domain.Job{{sampleJob("x")}},
	}
	jobs := make(chan domain.Job, 10)
	acks := make(chan struct{}, 10)

	c := consumer.New(src, jobs, acks)

	go func() {
		for j := range jobs {
			_ = j
			acks <- struct{}{}
		}
	}()

	err := c.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	close(jobs)
}
