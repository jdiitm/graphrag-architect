package bench

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

type benchJob struct {
	Key         []byte
	Value       []byte
	ValueReader io.Reader
	Topic       string
	Partition   int32
	Offset      int64
	Headers     map[string]string
	Timestamp   time.Time
}

func newBenchJob(payload string) benchJob {
	return benchJob{
		Key:       []byte("bench-key"),
		Value:     []byte(payload),
		Topic:     "raw-documents",
		Partition: 0,
		Offset:    1,
		Headers: map[string]string{
			"file_path":   "cmd/main.go",
			"source_type": "source_code",
			"repository":  "graphrag-architect",
			"tenant_id":   "bench-tenant",
		},
		Timestamp: time.Now(),
	}
}

func simulateMessageParse(job benchJob) (map[string]string, error) {
	result := make(map[string]string)
	result["topic"] = job.Topic
	result["key"] = string(job.Key)
	result["size"] = string(rune(len(job.Value)))
	for k, v := range job.Headers {
		result["header_"+k] = v
	}
	return result, nil
}

func simulateBatchParse(jobs []benchJob) ([]map[string]string, error) {
	results := make([]map[string]string, 0, len(jobs))
	for _, job := range jobs {
		r, err := simulateMessageParse(job)
		if err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, nil
}

func BenchmarkSingleMessageParse(b *testing.B) {
	payload := strings.Repeat("package main\nfunc main() {}\n", 100)
	job := newBenchJob(payload)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := simulateMessageParse(job)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchMessageParse(b *testing.B) {
	payload := strings.Repeat("package main\nfunc main() {}\n", 100)
	batchSizes := []int{10, 50, 100, 500}

	for _, size := range batchSizes {
		jobs := make([]benchJob, size)
		for i := range jobs {
			jobs[i] = newBenchJob(payload)
		}
		b.Run(string(rune(size))+"_messages", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := simulateBatchParse(jobs)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkConcurrentMessageParse(b *testing.B) {
	payload := strings.Repeat("package main\nfunc main() {}\n", 100)
	job := newBenchJob(payload)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		_ = ctx
		for pb.Next() {
			_, err := simulateMessageParse(job)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkLargePayloadParse(b *testing.B) {
	sizes := []int{1024, 10240, 102400, 1048576}
	for _, size := range sizes {
		payload := strings.Repeat("x", size)
		job := newBenchJob(payload)
		b.Run(string(rune(size))+"_bytes", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := simulateMessageParse(job)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
