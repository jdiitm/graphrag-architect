package processor

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/blobstore"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/telemetry"
)

const DefaultBlobThreshold = 64 * 1024

type blobPayload struct {
	FilePath    string `json:"file_path"`
	BlobKey     string `json:"blob_key"`
	Bucket      string `json:"bucket"`
	ContentType string `json:"content_type"`
	SizeBytes   int    `json:"size_bytes"`
	SourceType  string `json:"source_type"`
	Repository  string `json:"repository,omitempty"`
	CommitSHA   string `json:"commit_sha,omitempty"`
}

type BlobForwardingProcessor struct {
	producer  KafkaProducer
	store     blobstore.BlobStore
	topic     string
	bucket    string
	threshold int
}

func NewBlobForwardingProcessor(
	producer KafkaProducer,
	store blobstore.BlobStore,
	topic string,
	bucket string,
	threshold int,
) *BlobForwardingProcessor {
	if threshold <= 0 {
		threshold = DefaultBlobThreshold
	}
	return &BlobForwardingProcessor{
		producer:  producer,
		store:     store,
		topic:     topic,
		bucket:    bucket,
		threshold: threshold,
	}
}

func (p *BlobForwardingProcessor) Process(ctx context.Context, job domain.Job) error {
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

	if len(job.Value) < p.threshold {
		inline := &KafkaForwardingProcessor{producer: p.producer, topic: p.topic}
		return inline.Process(ctx, job)
	}

	blobKey := fmt.Sprintf("ingestion/%s/%s", job.Headers["commit_sha"], filePath)
	if _, err := p.store.Put(ctx, blobKey, job.Value); err != nil {
		return fmt.Errorf("blob store put: %w", err)
	}

	payload := blobPayload{
		FilePath:    filePath,
		BlobKey:     blobKey,
		Bucket:      p.bucket,
		ContentType: "application/octet-stream",
		SizeBytes:   len(job.Value),
		SourceType:  sourceType,
		Repository:  job.Headers["repository"],
		CommitSHA:   job.Headers["commit_sha"],
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal blob payload: %w", err)
	}

	headers := map[string]string{
		"file_path":   filePath,
		"source_type": sourceType,
		"blob_key":    blobKey,
	}

	return p.producer.Produce(ctx, p.topic, job.Key, data, headers)
}
