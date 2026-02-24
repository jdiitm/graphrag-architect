package processor

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/telemetry"
)

const (
	maxRetries       = 3
	initialBackoff   = 1 * time.Second
	backoffMultiplier = 2
)

type ingestDocument struct {
	FilePath   string  `json:"file_path"`
	Content    string  `json:"content"`
	SourceType string  `json:"source_type"`
	Repository *string `json:"repository,omitempty"`
	CommitSHA  *string `json:"commit_sha,omitempty"`
}

type ingestRequest struct {
	Documents []ingestDocument `json:"documents"`
}

type ForwardingProcessor struct {
	orchestratorURL string
	client          *http.Client
	authToken       string
	retryBackoff    time.Duration
}

type ForwardingOption func(*ForwardingProcessor)

func WithAuthToken(token string) ForwardingOption {
	return func(fp *ForwardingProcessor) {
		fp.authToken = token
	}
}

func WithRetryBackoff(d time.Duration) ForwardingOption {
	return func(fp *ForwardingProcessor) {
		fp.retryBackoff = d
	}
}

func NewForwardingProcessor(orchestratorURL string, client *http.Client, opts ...ForwardingOption) *ForwardingProcessor {
	fp := &ForwardingProcessor{
		orchestratorURL: orchestratorURL,
		client:          client,
		retryBackoff:    initialBackoff,
	}
	for _, o := range opts {
		o(fp)
	}
	return fp
}

func (f *ForwardingProcessor) Process(ctx context.Context, job domain.Job) error {
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

	doc := ingestDocument{
		FilePath:   filePath,
		Content:    base64.StdEncoding.EncodeToString(job.Value),
		SourceType: sourceType,
	}

	if repo, exists := job.Headers["repository"]; exists {
		doc.Repository = &repo
	}
	if sha, exists := job.Headers["commit_sha"]; exists {
		doc.CommitSHA = &sha
	}

	body, err := json.Marshal(ingestRequest{Documents: []ingestDocument{doc}})
	if err != nil {
		return fmt.Errorf("marshal ingest request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.orchestratorURL+"/ingest", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if f.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+f.authToken)
	}
	telemetry.InjectTraceContext(ctx, req.Header)

	var lastErr error
	backoff := f.retryBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= backoffMultiplier

			req, err = http.NewRequestWithContext(ctx, http.MethodPost, f.orchestratorURL+"/ingest", bytes.NewReader(body))
			if err != nil {
				return fmt.Errorf("create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")
			if f.authToken != "" {
				req.Header.Set("Authorization", "Bearer "+f.authToken)
			}
			telemetry.InjectTraceContext(ctx, req.Header)
		}

		resp, err := f.client.Do(req)
		if err != nil {
			return fmt.Errorf("forward to orchestrator: %w", err)
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			lastErr = fmt.Errorf("orchestrator returned 429")
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("orchestrator returned %d", resp.StatusCode)
		}

		return nil
	}

	return fmt.Errorf("max retries exceeded: %w", lastErr)
}
