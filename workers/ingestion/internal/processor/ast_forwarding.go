package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/parser"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/telemetry"
)

type ASTForwardingPayload struct {
	FilePath     string         `json:"file_path"`
	Language     string         `json:"language"`
	PackageName  string         `json:"package_name"`
	Functions    []FunctionInfo `json:"functions"`
	Imports      []string       `json:"imports"`
	HTTPCalls    []HTTPCallInfo `json:"http_calls,omitempty"`
	ServiceHints []string       `json:"service_hints,omitempty"`
	HTTPHandlers []string       `json:"http_handlers,omitempty"`
	SourceType   string         `json:"source_type"`
}

type ASTForwardingProcessor struct {
	producer KafkaProducer
	topic    string
	parser   *parser.Parser
}

func NewASTForwardingProcessor(producer KafkaProducer, topic string) *ASTForwardingProcessor {
	return &ASTForwardingProcessor{
		producer: producer,
		topic:    topic,
		parser:   parser.New(),
	}
}

func (p *ASTForwardingProcessor) Process(ctx context.Context, job domain.Job) error {
	ctx, span := telemetry.StartForwardSpan(ctx, job)
	defer span.End()

	filePath, ok := job.Headers["file_path"]
	if !ok || filePath == "" {
		return fmt.Errorf("missing required header: file_path")
	}

	ext := filepath.Ext(filePath)
	var payload *ASTForwardingPayload

	switch ext {
	case ".go":
		result, err := p.processGoFile(filePath, job.Value, job.Headers["source_type"])
		if err != nil {
			return err
		}
		payload = result
	case ".py":
		result, err := p.processPythonFile(filePath, job.Value, job.Headers["source_type"])
		if err != nil {
			return err
		}
		payload = result
	default:
		return nil
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal ast payload: %w", err)
	}

	headers := map[string]string{
		"file_path": filePath,
		"language":  payload.Language,
	}
	if st, exists := job.Headers["source_type"]; exists {
		headers["source_type"] = st
	}

	return p.producer.Produce(ctx, p.topic, job.Key, data, headers)
}

func (p *ASTForwardingProcessor) processGoFile(
	filePath string, content []byte, sourceType string,
) (*ASTForwardingPayload, error) {
	result, err := ExtractGoAST(filePath, string(content))
	if err != nil {
		return nil, fmt.Errorf("go ast extraction: %w", err)
	}

	return &ASTForwardingPayload{
		FilePath:     filePath,
		Language:     "go",
		PackageName:  result.PackageName,
		Functions:    result.Functions,
		Imports:      result.Imports,
		HTTPCalls:    result.HTTPCalls,
		ServiceHints: result.ServiceHints,
		SourceType:   sourceType,
	}, nil
}

func (p *ASTForwardingProcessor) processPythonFile(
	filePath string, content []byte, sourceType string,
) (*ASTForwardingPayload, error) {
	result, err := p.parser.ParsePythonFile(filePath, content)
	if err != nil {
		return nil, fmt.Errorf("python ast extraction: %w", err)
	}

	functions := make([]FunctionInfo, 0, len(result.Functions))
	for _, name := range result.Functions {
		functions = append(functions, FunctionInfo{Name: name, Exported: true})
	}

	return &ASTForwardingPayload{
		FilePath:     filePath,
		Language:     "python",
		PackageName:  result.PackageName,
		Functions:    functions,
		Imports:      result.Imports,
		HTTPHandlers: result.HTTPHandlers,
		SourceType:   sourceType,
	}, nil
}
