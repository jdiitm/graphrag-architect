package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

type ASTResult struct {
	Functions    []FunctionInfo `json:"functions"`
	Imports      []string       `json:"imports"`
	HTTPCalls    []HTTPCallInfo `json:"http_calls"`
	PackageName  string         `json:"package_name"`
	FilePath     string         `json:"file_path"`
	ServiceHints []string       `json:"service_hints"`
}

type FunctionInfo struct {
	Name       string `json:"name"`
	Exported   bool   `json:"exported"`
	Parameters int    `json:"parameters"`
}

type HTTPCallInfo struct {
	Method string `json:"method"`
	Path   string `json:"path_hint"`
}

type ASTProcessor struct {
	downstream DocumentProcessor
}

func NewASTProcessor(downstream DocumentProcessor) *ASTProcessor {
	return &ASTProcessor{downstream: downstream}
}

func (p *ASTProcessor) Process(ctx context.Context, job domain.Job) error {
	payload := make(map[string]string)
	if err := json.Unmarshal(job.Value, &payload); err != nil {
		return p.downstream.Process(ctx, job)
	}

	filePath, ok := payload["file_path"]
	if !ok || !strings.HasSuffix(filePath, ".go") {
		return p.downstream.Process(ctx, job)
	}

	content, ok := payload["content"]
	if !ok {
		return p.downstream.Process(ctx, job)
	}

	result, err := ExtractGoAST(filePath, content)
	if err != nil {
		return p.downstream.Process(ctx, job)
	}

	astJSON, err := json.Marshal(result)
	if err != nil {
		return p.downstream.Process(ctx, job)
	}

	enrichedPayload := make(map[string]interface{})
	for k, v := range payload {
		enrichedPayload[k] = v
	}
	enrichedPayload["ast_result"] = json.RawMessage(astJSON)

	enrichedValue, err := json.Marshal(enrichedPayload)
	if err != nil {
		return p.downstream.Process(ctx, job)
	}

	enrichedJob := domain.Job{
		Key:       job.Key,
		Value:     enrichedValue,
		Topic:     job.Topic,
		Partition: job.Partition,
		Offset:    job.Offset,
		Headers:   job.Headers,
		Timestamp: job.Timestamp,
	}

	return p.downstream.Process(ctx, enrichedJob)
}

func ExtractGoAST(filePath, content string) (*ASTResult, error) {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filePath, content, parser.AllErrors)
	if err != nil {
		return nil, fmt.Errorf("parse error for %s: %w", filePath, err)
	}

	result := &ASTResult{
		FilePath:    filePath,
		PackageName: node.Name.Name,
	}

	for _, imp := range node.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		result.Imports = append(result.Imports, path)
		if strings.Contains(path, "net/http") {
			result.ServiceHints = append(result.ServiceHints, "http-server")
		}
		if strings.Contains(path, "google.golang.org/grpc") {
			result.ServiceHints = append(result.ServiceHints, "grpc-server")
		}
	}

	for _, decl := range node.Decls {
		funcDecl, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		info := FunctionInfo{
			Name:     funcDecl.Name.Name,
			Exported: funcDecl.Name.IsExported(),
		}
		if funcDecl.Type.Params != nil {
			info.Parameters = funcDecl.Type.Params.NumFields()
		}
		result.Functions = append(result.Functions, info)

		extractHTTPCalls(funcDecl, result)
	}

	return result, nil
}

func extractHTTPCalls(funcDecl *ast.FuncDecl, result *ASTResult) {
	ast.Inspect(funcDecl, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		methodName := sel.Sel.Name
		httpMethods := map[string]string{
			"Get":    "GET",
			"Post":   "POST",
			"Put":    "PUT",
			"Delete": "DELETE",
		}
		if method, found := httpMethods[methodName]; found {
			hint := HTTPCallInfo{Method: method}
			if len(call.Args) > 0 {
				if lit, ok := call.Args[0].(*ast.BasicLit); ok {
					hint.Path = strings.Trim(lit.Value, `"`)
				}
			}
			result.HTTPCalls = append(result.HTTPCalls, hint)
		}
		return true
	})
}
