package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaDLQSink struct {
	client *kgo.Client
	topic  string
}

func dlqProducerOpts(brokers, topic string) []kgo.Opt {
	seeds := strings.Split(brokers, ",")
	return []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(producerRetries),
		kgo.RecordDeliveryTimeout(producerDeliveryTimeout),
	}
}

func NewKafkaDLQSink(brokers, topic string) (*KafkaDLQSink, error) {
	client, err := kgo.NewClient(dlqProducerOpts(brokers, topic)...)
	if err != nil {
		return nil, fmt.Errorf("create dlq producer: %w", err)
	}
	return &KafkaDLQSink{client: client, topic: topic}, nil
}

func buildDLQRecord(r domain.Result) *kgo.Record {
	headers := []kgo.RecordHeader{
		{Key: "source_topic", Value: []byte(r.Job.Topic)},
		{Key: "source_partition", Value: []byte(strconv.FormatInt(int64(r.Job.Partition), 10))},
		{Key: "source_offset", Value: []byte(strconv.FormatInt(r.Job.Offset, 10))},
		{Key: "attempts", Value: []byte(strconv.Itoa(r.Attempts))},
		{Key: "failed_at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	}
	if r.Err != nil {
		headers = append(headers, kgo.RecordHeader{
			Key: "error", Value: []byte(r.Err.Error()),
		})
	}
	return &kgo.Record{
		Key:     r.Job.Key,
		Value:   r.Job.Value,
		Headers: headers,
	}
}

func (k *KafkaDLQSink) Send(ctx context.Context, r domain.Result) error {
	record := buildDLQRecord(r)
	result := k.client.ProduceSync(ctx, record)
	if err := result.FirstErr(); err != nil {
		return fmt.Errorf("dlq produce to %s: %w", k.topic, err)
	}
	log.Printf("DLQ: published to %s topic=%s partition=%d offset=%d",
		k.topic, r.Job.Topic, r.Job.Partition, r.Job.Offset)
	return nil
}

func (k *KafkaDLQSink) Close() {
	k.client.Close()
}
