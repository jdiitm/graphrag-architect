package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/consumer"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaJobSource struct {
	client *kgo.Client
}

func NewKafkaJobSource(brokers, topic, group string) *KafkaJobSource {
	seeds := strings.Split(brokers, ",")
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(30*time.Second),
		kgo.RebalanceTimeout(60*time.Second),
		kgo.HeartbeatInterval(3*time.Second),
	)
	if err != nil {
		log.Fatalf("create kafka client: %v", err)
	}
	return &KafkaJobSource{client: client}
}

func (s *KafkaJobSource) Poll(ctx context.Context) ([]domain.Job, error) {
	fetches := s.client.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		for _, e := range errs {
			if e.Err == context.Canceled || e.Err == context.DeadlineExceeded {
				return nil, consumer.ErrSourceClosed
			}
		}
		return nil, fmt.Errorf("kafka poll: %v", fetches.Errors())
	}

	var jobs []domain.Job
	fetches.EachRecord(func(r *kgo.Record) {
		headers := make(map[string]string, len(r.Headers))
		for _, h := range r.Headers {
			headers[h.Key] = string(h.Value)
		}
		jobs = append(jobs, domain.Job{
			Key:       r.Key,
			Value:     r.Value,
			Topic:     r.Topic,
			Partition: r.Partition,
			Offset:    r.Offset,
			Headers:   headers,
			Timestamp: r.Timestamp,
		})
	})

	if len(jobs) == 0 {
		return nil, consumer.ErrSourceClosed
	}

	return jobs, nil
}

func (s *KafkaJobSource) Commit(ctx context.Context) error {
	s.client.AllowRebalance()
	return s.client.CommitUncommittedOffsets(ctx)
}

func (s *KafkaJobSource) Close() {
	s.client.Close()
}

type KafkaProducerClient struct {
	client *kgo.Client
}

func NewKafkaProducerClient(brokers, topic string) (*KafkaProducerClient, error) {
	seeds := strings.Split(brokers, ",")
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &KafkaProducerClient{client: client}, nil
}

func (p *KafkaProducerClient) Produce(ctx context.Context, topic string, key, value []byte, headers map[string]string) error {
	kgoHeaders := make([]kgo.RecordHeader, 0, len(headers))
	for k, v := range headers {
		kgoHeaders = append(kgoHeaders, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	record := &kgo.Record{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: kgoHeaders,
	}
	result := p.client.ProduceSync(ctx, record)
	return result.FirstErr()
}

func (p *KafkaProducerClient) Close() {
	p.client.Close()
}

type LogDLQSink struct{}

func (l *LogDLQSink) Send(_ context.Context, r domain.Result) error {
	log.Printf("DLQ: job topic=%s partition=%d offset=%d err=%v attempts=%d",
		r.Job.Topic, r.Job.Partition, r.Job.Offset, r.Err, r.Attempts)
	return nil
}
