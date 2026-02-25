package main

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

func buildTestResult() domain.Result {
	return domain.Result{
		Job: domain.Job{
			Key:       []byte("repo-hash-123"),
			Value:     []byte(`{"file_path":"main.go","content":"cGFja2FnZSBtYWlu"}`),
			Topic:     "raw-documents",
			Partition: 2,
			Offset:    42,
			Headers:   map[string]string{"traceparent": "00-abc-def-01"},
			Timestamp: time.Now(),
		},
		Err:      errors.New("orchestrator returned 503"),
		Attempts: 3,
	}
}

func TestBuildDLQRecord_PreservesOriginalPayload(t *testing.T) {
	r := buildTestResult()
	record := buildDLQRecord(r)

	if string(record.Key) != string(r.Job.Key) {
		t.Errorf("key mismatch: got %q, want %q", record.Key, r.Job.Key)
	}
	if string(record.Value) != string(r.Job.Value) {
		t.Errorf("value mismatch: got %q, want %q", record.Value, r.Job.Value)
	}
}

func TestBuildDLQRecord_IncludesErrorMetadata(t *testing.T) {
	r := buildTestResult()
	record := buildDLQRecord(r)

	headerMap := make(map[string]string, len(record.Headers))
	for _, h := range record.Headers {
		headerMap[h.Key] = string(h.Value)
	}

	requiredHeaders := []string{
		"error", "attempts", "source_topic", "source_partition",
		"source_offset", "failed_at",
	}
	for _, key := range requiredHeaders {
		if _, ok := headerMap[key]; !ok {
			t.Errorf("missing required header %q", key)
		}
	}

	if headerMap["error"] != "orchestrator returned 503" {
		t.Errorf("error header: got %q, want %q",
			headerMap["error"], "orchestrator returned 503")
	}
	if headerMap["attempts"] != "3" {
		t.Errorf("attempts header: got %q, want %q", headerMap["attempts"], "3")
	}
	if headerMap["source_topic"] != "raw-documents" {
		t.Errorf("source_topic header: got %q, want %q",
			headerMap["source_topic"], "raw-documents")
	}
	if headerMap["source_partition"] != strconv.FormatInt(int64(r.Job.Partition), 10) {
		t.Errorf("source_partition header mismatch")
	}
	if headerMap["source_offset"] != "42" {
		t.Errorf("source_offset header: got %q, want %q",
			headerMap["source_offset"], "42")
	}

	_, err := time.Parse(time.RFC3339, headerMap["failed_at"])
	if err != nil {
		t.Errorf("failed_at not valid RFC3339: %v", err)
	}
}

func TestBuildDLQRecord_NoErrorHeader_WhenNilErr(t *testing.T) {
	r := buildTestResult()
	r.Err = nil
	record := buildDLQRecord(r)

	for _, h := range record.Headers {
		if h.Key == "error" {
			t.Error("expected no error header when Err is nil")
		}
	}
}

func TestKafkaDLQSink_ImplementsDeadLetterSink(t *testing.T) {
	var _ interface {
		Send(context.Context, domain.Result) error
	} = &KafkaDLQSink{}
}
