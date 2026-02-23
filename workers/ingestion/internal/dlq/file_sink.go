package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

type FileSinkRecord struct {
	Key       []byte            `json:"key"`
	Value     []byte            `json:"value"`
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Headers   map[string]string `json:"headers"`
	Error     string            `json:"error"`
	Attempts  int               `json:"attempts"`
	Timestamp time.Time         `json:"timestamp"`
	WrittenAt time.Time         `json:"written_at"`
}

type FileSink struct {
	mu   sync.Mutex
	path string
}

func NewFileSink(path string) (*FileSink, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create dlq fallback directory: %w", err)
	}
	return &FileSink{path: path}, nil
}

func (f *FileSink) Send(_ context.Context, result domain.Result) error {
	record := FileSinkRecord{
		Key:       result.Job.Key,
		Value:     result.Job.Value,
		Topic:     result.Job.Topic,
		Partition: result.Job.Partition,
		Offset:    result.Job.Offset,
		Headers:   result.Job.Headers,
		Attempts:  result.Attempts,
		Timestamp: result.Job.Timestamp,
		WrittenAt: time.Now().UTC(),
	}
	if result.Err != nil {
		record.Error = result.Err.Error()
	}

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal dlq record: %w", err)
	}
	data = append(data, '\n')

	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := os.OpenFile(f.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("open dlq fallback file: %w", err)
	}
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write dlq fallback record: %w", err)
	}
	return nil
}
