package benchmarks_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type benchJob struct {
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Topic     string            `json:"topic"`
	Headers   map[string]string `json:"headers"`
	Timestamp time.Time         `json:"timestamp"`
}

func goSourceBench(nFuncs int) string {
	src := "package bench\n\nimport \"net/http\"\n\n"
	for i := range nFuncs {
		src += fmt.Sprintf(
			"func Handler%d(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }\n\n", i,
		)
	}
	return src
}

func BenchmarkJobSerialization_Small(b *testing.B) {
	job := benchJob{
		Key:   "bench-key",
		Value: goSourceBench(10),
		Topic: "raw-documents",
		Headers: map[string]string{
			"file_path":   "cmd/main.go",
			"source_type": "source_code",
			"repository":  "graphrag-architect",
		},
		Timestamp: time.Now(),
	}
	b.ResetTimer()
	for range b.N {
		data, _ := json.Marshal(job)
		_ = data
	}
}

func BenchmarkJobSerialization_Large(b *testing.B) {
	job := benchJob{
		Key:   "bench-key-large",
		Value: goSourceBench(500),
		Topic: "raw-documents",
		Headers: map[string]string{
			"file_path":   "internal/service/handler.go",
			"source_type": "source_code",
			"repository":  "graphrag-architect",
			"commit_sha":  "abc123def456",
		},
		Timestamp: time.Now(),
	}
	b.ResetTimer()
	for range b.N {
		data, _ := json.Marshal(job)
		_ = data
	}
}

func BenchmarkHTTPForwarding(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"committed","entities_extracted":2,"errors":[]}`))
	}))
	defer srv.Close()

	client := srv.Client()
	payload := []byte(`{"key":"bench","value":"package main","topic":"raw-documents","headers":{"file_path":"main.go"}}`)

	b.ResetTimer()
	for range b.N {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, nil)
		req.Body = io.NopCloser(bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}
}

func BenchmarkJobDeserialization(b *testing.B) {
	job := benchJob{
		Key:   "bench-deser",
		Value: goSourceBench(50),
		Topic: "raw-documents",
		Headers: map[string]string{
			"file_path":   "cmd/main.go",
			"source_type": "source_code",
		},
		Timestamp: time.Now(),
	}
	data, _ := json.Marshal(job)
	b.ResetTimer()
	for range b.N {
		var out benchJob
		_ = json.Unmarshal(data, &out)
	}
}
