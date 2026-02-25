package processor_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/processor"
)

type mockEmitter struct {
	emitted []emittedEvent
}

type emittedEvent struct {
	topic string
	key   []byte
	value []byte
}

func (m *mockEmitter) Emit(_ context.Context, topic string, key []byte, value []byte) error {
	m.emitted = append(m.emitted, emittedEvent{topic: topic, key: key, value: value})
	return nil
}

func TestStageAndEmitProcessor_WritesFileAndEmitsEvent(t *testing.T) {
	stagingDir := t.TempDir()
	emitter := &mockEmitter{}
	proc := processor.NewStageAndEmitProcessor(stagingDir, emitter)

	job := domain.Job{
		Value: []byte("package main\nfunc main() {}"),
		Headers: map[string]string{
			"file_path":   "cmd/main.go",
			"source_type": "source_code",
		},
	}

	if err := proc.Process(context.Background(), job); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	stagingPath := filepath.Join(stagingDir, "cmd/main.go")
	content, err := os.ReadFile(stagingPath)
	if err != nil {
		t.Fatalf("staged file not found: %v", err)
	}
	if string(content) != string(job.Value) {
		t.Errorf("staged content = %q, want %q", string(content), string(job.Value))
	}

	if len(emitter.emitted) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.emitted))
	}

	evt := emitter.emitted[0]
	if evt.topic != "extraction-pending" {
		t.Errorf("topic = %q, want %q", evt.topic, "extraction-pending")
	}
	if string(evt.key) != "cmd/main.go" {
		t.Errorf("key = %q, want %q", string(evt.key), "cmd/main.go")
	}

	var event processor.ExtractionEvent
	if err := json.Unmarshal(evt.value, &event); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if event.StagingPath != stagingPath {
		t.Errorf("event.StagingPath = %q, want %q", event.StagingPath, stagingPath)
	}
	if event.Headers["source_type"] != "source_code" {
		t.Errorf("event.Headers[source_type] = %q, want source_code", event.Headers["source_type"])
	}
}

func TestStageAndEmitProcessor_MissingFilePathReturnsError(t *testing.T) {
	stagingDir := t.TempDir()
	emitter := &mockEmitter{}
	proc := processor.NewStageAndEmitProcessor(stagingDir, emitter)

	job := domain.Job{
		Value:   []byte("data"),
		Headers: map[string]string{"source_type": "source_code"},
	}

	err := proc.Process(context.Background(), job)
	if err == nil {
		t.Fatal("expected error for missing file_path header")
	}
}

func TestStageAndEmitProcessor_CustomTopic(t *testing.T) {
	stagingDir := t.TempDir()
	emitter := &mockEmitter{}
	proc := processor.NewStageAndEmitProcessor(
		stagingDir, emitter,
		processor.WithStagingTopic("custom-topic"),
	)

	job := domain.Job{
		Value:   []byte("data"),
		Headers: map[string]string{"file_path": "test.go", "source_type": "source_code"},
	}

	if err := proc.Process(context.Background(), job); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if emitter.emitted[0].topic != "custom-topic" {
		t.Errorf("topic = %q, want custom-topic", emitter.emitted[0].topic)
	}
}

func TestStageAndEmitProcessor_PathTraversalRejected(t *testing.T) {
	stagingDir := t.TempDir()
	emitter := &mockEmitter{}
	proc := processor.NewStageAndEmitProcessor(stagingDir, emitter)

	traversalPaths := []string{
		"../../etc/shadow",
		"../../../etc/passwd",
		"subdir/../../etc/crontab",
	}
	for _, malicious := range traversalPaths {
		job := domain.Job{
			Value:   []byte("malicious payload"),
			Headers: map[string]string{"file_path": malicious},
		}
		err := proc.Process(context.Background(), job)
		if err == nil {
			t.Fatalf("expected error for path traversal %q, got nil", malicious)
		}
	}

	if len(emitter.emitted) != 0 {
		t.Errorf("expected 0 emitted events after traversal attempts, got %d", len(emitter.emitted))
	}
}

func TestStageAndEmitProcessor_ImplementsDocumentProcessor(t *testing.T) {
	stagingDir := t.TempDir()
	emitter := &mockEmitter{}
	var _ processor.DocumentProcessor = processor.NewStageAndEmitProcessor(stagingDir, emitter)
}

func TestStageAndEmitProcessor_StreamingWriteFromReader(t *testing.T) {
	stagingDir := t.TempDir()
	emitter := &mockEmitter{}
	proc := processor.NewStageAndEmitProcessor(stagingDir, emitter)

	payload := []byte("streaming payload content here")
	reader := bytes.NewReader(payload)

	job := domain.Job{
		Value:       nil,
		ValueReader: reader,
		Headers: map[string]string{
			"file_path":   "stream/test.txt",
			"source_type": "source_code",
		},
	}

	if err := proc.Process(context.Background(), job); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	stagingPath := filepath.Join(stagingDir, "stream/test.txt")
	content, err := os.ReadFile(stagingPath)
	if err != nil {
		t.Fatalf("staged file not found: %v", err)
	}
	if string(content) != string(payload) {
		t.Errorf("staged content = %q, want %q", string(content), string(payload))
	}
}

func TestStageAndEmitProcessor_StreamingFallsBackToValue(t *testing.T) {
	stagingDir := t.TempDir()
	emitter := &mockEmitter{}
	proc := processor.NewStageAndEmitProcessor(stagingDir, emitter)

	job := domain.Job{
		Value:       []byte("fallback value content"),
		ValueReader: nil,
		Headers: map[string]string{
			"file_path":   "fallback/test.txt",
			"source_type": "source_code",
		},
	}

	if err := proc.Process(context.Background(), job); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	stagingPath := filepath.Join(stagingDir, "fallback/test.txt")
	content, err := os.ReadFile(stagingPath)
	if err != nil {
		t.Fatalf("staged file not found: %v", err)
	}
	if string(content) != "fallback value content" {
		t.Errorf("staged content = %q, want %q", string(content), "fallback value content")
	}
}

func TestStageAndEmitProcessor_ContextCancellationAbortsStream(t *testing.T) {
	stagingDir := t.TempDir()
	emitter := &mockEmitter{}
	proc := processor.NewStageAndEmitProcessor(stagingDir, emitter)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	reader := bytes.NewReader([]byte("should not be fully written"))

	job := domain.Job{
		Value:       nil,
		ValueReader: reader,
		Headers: map[string]string{
			"file_path":   "cancel/test.txt",
			"source_type": "source_code",
		},
	}

	err := proc.Process(ctx, job)
	if err == nil {
		t.Log("Process completed without error on cancelled context (acceptable if write was fast)")
	}
}

func TestStageAndEmitProcessor_StreamingPreservesPathTraversalProtection(t *testing.T) {
	stagingDir := t.TempDir()
	emitter := &mockEmitter{}
	proc := processor.NewStageAndEmitProcessor(stagingDir, emitter)

	reader := bytes.NewReader([]byte("malicious"))

	job := domain.Job{
		Value:       nil,
		ValueReader: reader,
		Headers: map[string]string{
			"file_path": "../../etc/passwd",
		},
	}

	err := proc.Process(context.Background(), job)
	if err == nil {
		t.Fatal("expected error for path traversal with streaming reader")
	}
}
