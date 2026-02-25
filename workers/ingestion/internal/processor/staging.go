package processor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/telemetry"
)

type ExtractionEvent struct {
	StagingPath string            `json:"staging_path"`
	Headers     map[string]string `json:"headers"`
}

type EventEmitter interface {
	Emit(ctx context.Context, topic string, key []byte, value []byte) error
}

type StageAndEmitProcessor struct {
	stagingDir string
	emitter    EventEmitter
	topic      string
}

type StagingOption func(*StageAndEmitProcessor)

func WithStagingTopic(topic string) StagingOption {
	return func(p *StageAndEmitProcessor) {
		p.topic = topic
	}
}

func NewStageAndEmitProcessor(stagingDir string, emitter EventEmitter, opts ...StagingOption) *StageAndEmitProcessor {
	p := &StageAndEmitProcessor{
		stagingDir: stagingDir,
		emitter:    emitter,
		topic:      "extraction-pending",
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

func (s *StageAndEmitProcessor) Process(ctx context.Context, job domain.Job) error {
	filePath, ok := job.Headers["file_path"]
	if !ok {
		return fmt.Errorf("missing required header: file_path")
	}

	stagingPath := filepath.Join(s.stagingDir, filepath.Clean(filePath))

	ctx, span := telemetry.StartStagingSpan(ctx, job, stagingPath)
	defer span.End()
	absStagingDir, err := filepath.Abs(s.stagingDir)
	if err != nil {
		return fmt.Errorf("resolve staging directory: %w", err)
	}
	absStagingPath, err := filepath.Abs(stagingPath)
	if err != nil {
		return fmt.Errorf("resolve staging path: %w", err)
	}
	if !strings.HasPrefix(absStagingPath, absStagingDir+string(filepath.Separator)) {
		return fmt.Errorf("path traversal detected: %s", filePath)
	}

	if err := os.MkdirAll(filepath.Dir(stagingPath), 0o755); err != nil {
		return fmt.Errorf("create staging directory: %w", err)
	}
	if err := writeStaged(ctx, stagingPath, job); err != nil {
		return fmt.Errorf("write staging file: %w", err)
	}

	event := ExtractionEvent{
		StagingPath: stagingPath,
		Headers:     job.Headers,
	}
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal extraction event: %w", err)
	}

	if err := s.emitter.Emit(ctx, s.topic, []byte(filePath), eventBytes); err != nil {
		return fmt.Errorf("emit extraction event: %w", err)
	}

	return nil
}

func writeStaged(ctx context.Context, path string, job domain.Job) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	var src io.Reader
	if job.ValueReader != nil {
		src = job.ValueReader
	} else {
		src = bytes.NewReader(job.Value)
	}

	bw := bufio.NewWriter(f)
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := io.Copy(bw, src); err != nil {
		return err
	}
	return bw.Flush()
}
