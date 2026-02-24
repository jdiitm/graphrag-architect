package processor_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/processor"
)

type fakeProducer struct {
	produced []producedRecord
	err      error
}

type producedRecord struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

func (f *fakeProducer) Produce(_ context.Context, topic string, key, value []byte, headers map[string]string) error {
	if f.err != nil {
		return f.err
	}
	f.produced = append(f.produced, producedRecord{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: headers,
	})
	return nil
}

func kafkaJob() domain.Job {
	return domain.Job{
		Key:   []byte("repo-hash-abc"),
		Value: []byte("package main\n\nfunc main() {}"),
		Topic: "raw-documents",
		Headers: map[string]string{
			"file_path":   "cmd/main.go",
			"source_type": "source_code",
			"repository":  "graphrag-architect",
			"commit_sha":  "abc123",
		},
	}
}

func TestKafkaForwardingProcessor_Process(t *testing.T) {
	producer := &fakeProducer{}
	p := processor.NewKafkaForwardingProcessor(producer, "graphrag.parsed")
	job := kafkaJob()

	if err := p.Process(context.Background(), job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(producer.produced) != 1 {
		t.Fatalf("expected 1 produced record, got %d", len(producer.produced))
	}

	rec := producer.produced[0]
	if rec.Topic != "graphrag.parsed" {
		t.Errorf("topic = %q, want %q", rec.Topic, "graphrag.parsed")
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(rec.Value, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["file_path"] != "cmd/main.go" {
		t.Errorf("file_path = %v, want %q", payload["file_path"], "cmd/main.go")
	}
	if payload["content"] != "package main\n\nfunc main() {}" {
		t.Errorf("content is raw text, not base64")
	}
	if payload["source_type"] != "source_code" {
		t.Errorf("source_type = %v, want %q", payload["source_type"], "source_code")
	}
}

func TestKafkaForwardingProcessor_MissingHeaders(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		wantErr string
	}{
		{
			name:    "missing file_path",
			headers: map[string]string{"source_type": "code"},
			wantErr: "missing required header: file_path",
		},
		{
			name:    "missing source_type",
			headers: map[string]string{"file_path": "main.go"},
			wantErr: "missing required header: source_type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := processor.NewKafkaForwardingProcessor(&fakeProducer{}, "graphrag.parsed")
			job := domain.Job{
				Key:     []byte("key"),
				Value:   []byte("content"),
				Headers: tt.headers,
			}
			err := p.Process(context.Background(), job)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestKafkaForwardingProcessor_ProducerError(t *testing.T) {
	producer := &fakeProducer{err: fmt.Errorf("kafka unavailable")}
	p := processor.NewKafkaForwardingProcessor(producer, "graphrag.parsed")

	err := p.Process(context.Background(), kafkaJob())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestKafkaForwardingProcessor_WritesRawContent(t *testing.T) {
	producer := &fakeProducer{}
	p := processor.NewKafkaForwardingProcessor(producer, "graphrag.parsed")
	rawContent := "apiVersion: v1\nkind: Service\nmetadata:\n  name: auth"
	job := domain.Job{
		Key:   []byte("manifest-key"),
		Value: []byte(rawContent),
		Headers: map[string]string{
			"file_path":   "k8s/auth-svc.yaml",
			"source_type": "k8s_manifest",
		},
	}

	if err := p.Process(context.Background(), job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(producer.produced[0].Value, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if payload["content"] != rawContent {
		t.Errorf("expected raw content in payload, not base64 encoded")
	}
}
