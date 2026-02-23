package dlq_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dlq"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

func testResult(key string) domain.Result {
	return domain.Result{
		Job: domain.Job{
			Key:       []byte(key),
			Value:     []byte(`{"file":"main.go"}`),
			Topic:     "raw-documents",
			Partition: 1,
			Offset:    99,
			Headers:   map[string]string{"traceparent": "00-abc-def-01"},
			Timestamp: time.Date(2026, 2, 23, 12, 0, 0, 0, time.UTC),
		},
		Err:      errors.New("processing failed"),
		Attempts: 3,
	}
}

func TestFileSinkWritesResultToFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dlq-fallback.jsonl")

	sink, err := dlq.NewFileSink(path)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}

	result := testResult("repo-123")
	if err := sink.Send(context.Background(), result); err != nil {
		t.Fatalf("Send: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("lines = %d, want 1", len(lines))
	}
	var record dlq.FileSinkRecord
	if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if string(record.Key) != "repo-123" {
		t.Errorf("key = %q, want %q", string(record.Key), "repo-123")
	}
	if record.Error != "processing failed" {
		t.Errorf("error = %q, want %q", record.Error, "processing failed")
	}
	if record.Attempts != 3 {
		t.Errorf("attempts = %d, want 3", record.Attempts)
	}
	if record.Topic != "raw-documents" {
		t.Errorf("topic = %q, want %q", record.Topic, "raw-documents")
	}
}

func TestFileSinkAppendsMultipleResults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dlq-fallback.jsonl")

	sink, err := dlq.NewFileSink(path)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}

	for _, key := range []string{"msg-1", "msg-2", "msg-3"} {
		if err := sink.Send(context.Background(), testResult(key)); err != nil {
			t.Fatalf("Send(%s): %v", key, err)
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines = %d, want 3", len(lines))
	}

	for i, key := range []string{"msg-1", "msg-2", "msg-3"} {
		var record dlq.FileSinkRecord
		if err := json.Unmarshal([]byte(lines[i]), &record); err != nil {
			t.Fatalf("Unmarshal line %d: %v", i, err)
		}
		if string(record.Key) != key {
			t.Errorf("line %d: key = %q, want %q", i, record.Key, key)
		}
	}
}

func TestFileSinkCreatesParentDirectories(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "deep", "dlq-fallback.jsonl")

	sink, err := dlq.NewFileSink(path)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}

	if err := sink.Send(context.Background(), testResult("nested-msg")); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("expected file to exist after Send")
	}
}

func TestFileSinkImplementsDeadLetterSink(t *testing.T) {
	dir := t.TempDir()
	sink, err := dlq.NewFileSink(filepath.Join(dir, "test.jsonl"))
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	var _ dlq.DeadLetterSink = sink
}
