package processor_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/blobstore"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/processor"
)

func TestBlobForwardingProcessor_SmallContentInline(t *testing.T) {
	store := blobstore.NewInMemoryBlobStore()
	producer := &fakeProducer{}
	p := processor.NewBlobForwardingProcessor(producer, store, "graphrag.parsed", "test-bucket", 1024)

	job := domain.Job{
		Key:   []byte("key1"),
		Value: []byte("package main"),
		Headers: map[string]string{
			"file_path":   "main.go",
			"source_type": "source_code",
		},
	}

	if err := p.Process(context.Background(), job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(producer.produced) != 1 {
		t.Fatalf("expected 1 produced record, got %d", len(producer.produced))
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(producer.produced[0].Value, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, hasBlobKey := payload["blob_key"]; hasBlobKey {
		t.Error("small content should be inlined, not stored as blob")
	}
	if payload["content"] != "package main" {
		t.Errorf("expected inline content, got %v", payload["content"])
	}
}

func TestBlobForwardingProcessor_LargeContentUploaded(t *testing.T) {
	store := blobstore.NewInMemoryBlobStore()
	producer := &fakeProducer{}
	threshold := 64
	p := processor.NewBlobForwardingProcessor(producer, store, "graphrag.parsed", "test-bucket", threshold)

	largeContent := strings.Repeat("x", threshold+1)
	job := domain.Job{
		Key:   []byte("key2"),
		Value: []byte(largeContent),
		Headers: map[string]string{
			"file_path":   "big/file.go",
			"source_type": "source_code",
			"commit_sha":  "abc123",
		},
	}

	if err := p.Process(context.Background(), job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(producer.produced[0].Value, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	blobKey, ok := payload["blob_key"].(string)
	if !ok || blobKey == "" {
		t.Fatal("expected blob_key in payload for large content")
	}
	if payload["bucket"] != "test-bucket" {
		t.Errorf("bucket = %v, want test-bucket", payload["bucket"])
	}

	ctx := context.Background()
	stored, err := store.Get(ctx, blobKey)
	if err != nil {
		t.Fatalf("blob not found in store: %v", err)
	}
	if string(stored) != largeContent {
		t.Error("stored blob content does not match original")
	}

	if _, hasContent := payload["content"]; hasContent {
		t.Error("large content should not be inlined in kafka message")
	}
}

func TestBlobForwardingProcessor_MissingHeaders(t *testing.T) {
	store := blobstore.NewInMemoryBlobStore()
	producer := &fakeProducer{}
	p := processor.NewBlobForwardingProcessor(producer, store, "topic", "bucket", 64)

	job := domain.Job{
		Key:     []byte("key"),
		Value:   []byte("data"),
		Headers: map[string]string{},
	}

	err := p.Process(context.Background(), job)
	if err == nil {
		t.Fatal("expected error for missing headers")
	}
}

func TestBlobForwardingProcessor_BlobKeyInHeaders(t *testing.T) {
	store := blobstore.NewInMemoryBlobStore()
	producer := &fakeProducer{}
	p := processor.NewBlobForwardingProcessor(producer, store, "graphrag.parsed", "bucket", 8)

	job := domain.Job{
		Key:   []byte("key"),
		Value: []byte("large enough content"),
		Headers: map[string]string{
			"file_path":   "f.go",
			"source_type": "source_code",
			"commit_sha":  "def456",
		},
	}

	if err := p.Process(context.Background(), job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rec := producer.produced[0]
	if rec.Headers["blob_key"] == "" {
		t.Error("expected blob_key in kafka message headers")
	}
}
