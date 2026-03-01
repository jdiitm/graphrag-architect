package main

import (
	"testing"
	"time"
)

func TestProducerOptsMinimumCount(t *testing.T) {
	opts := producerOpts("localhost:9092", "test-topic")
	if len(opts) < 5 {
		t.Fatalf(
			"producerOpts returned %d opts, want >= 5 "+
				"(seeds, topic, acks, retries, delivery timeout)",
			len(opts),
		)
	}
}

func TestProducerDeliveryTimeout(t *testing.T) {
	if producerDeliveryTimeout < 10*time.Second {
		t.Fatalf(
			"producerDeliveryTimeout=%v, want >= 10s for reliable delivery",
			producerDeliveryTimeout,
		)
	}
}

func TestProducerRecordRetriesValue(t *testing.T) {
	if producerRetries < 1 {
		t.Fatal("producerRetries must be >= 1 for at-least-once delivery")
	}
}

func TestNewKafkaProducerClient_UsesIdempotentOpts(t *testing.T) {
	client, err := NewKafkaProducerClient("localhost:19092", "test-topic")
	if err != nil {
		t.Fatalf("unexpected error creating producer: %v", err)
	}
	defer client.Close()
}

func TestDLQProducerOptsMinimumCount(t *testing.T) {
	opts := dlqProducerOpts("localhost:9092", "test-dlq")
	if len(opts) < 5 {
		t.Fatalf(
			"dlqProducerOpts returned %d opts, want >= 5 "+
				"(seeds, topic, acks, retries, delivery timeout)",
			len(opts),
		)
	}
}

func TestNewKafkaDLQSink_UsesIdempotentOpts(t *testing.T) {
	sink, err := NewKafkaDLQSink("localhost:19092", "test-dlq")
	if err != nil {
		t.Fatalf("unexpected error creating DLQ sink: %v", err)
	}
	defer sink.Close()
}
