package processor

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/telemetry"
)

type KafkaProducer interface {
	Produce(ctx context.Context, topic string, key, value []byte, headers map[string]string) error
}

type parsedPayload struct {
	FilePath   string `json:"file_path"`
	Content    string `json:"content"`
	SourceType string `json:"source_type"`
	Repository string `json:"repository,omitempty"`
	CommitSHA  string `json:"commit_sha,omitempty"`
}

type KafkaForwardingProcessor struct {
	producer KafkaProducer
	topic    string
}

func NewKafkaForwardingProcessor(producer KafkaProducer, topic string) *KafkaForwardingProcessor {
	return &KafkaForwardingProcessor{
		producer: producer,
		topic:    topic,
	}
}

func (p *KafkaForwardingProcessor) Process(ctx context.Context, job domain.Job) error {
	ctx, span := telemetry.StartForwardSpan(ctx, job)
	defer span.End()

	filePath, ok := job.Headers["file_path"]
	if !ok {
		return fmt.Errorf("missing required header: file_path")
	}
	sourceType, ok := job.Headers["source_type"]
	if !ok {
		return fmt.Errorf("missing required header: source_type")
	}

	payload := parsedPayload{
		FilePath:   filePath,
		Content:    string(job.Value),
		SourceType: sourceType,
		Repository: job.Headers["repository"],
		CommitSHA:  job.Headers["commit_sha"],
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal parsed payload: %w", err)
	}

	headers := map[string]string{
		"file_path":   filePath,
		"source_type": sourceType,
	}

	return p.producer.Produce(ctx, p.topic, job.Key, data, headers)
}
