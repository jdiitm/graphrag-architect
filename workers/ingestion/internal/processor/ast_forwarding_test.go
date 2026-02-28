package processor_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/processor"
)

func goSourceJob() domain.Job {
	src := `package main

import (
	"fmt"
	"net/http"
)

func main() {
	http.ListenAndServe(":8080", nil)
}

func HandleHealth(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "ok")
}
`
	return domain.Job{
		Key:   []byte("repo-abc-go"),
		Value: []byte(src),
		Topic: "raw-documents",
		Headers: map[string]string{
			"file_path":   "cmd/server/main.go",
			"source_type": "source_code",
		},
	}
}

func pythonSourceJob() domain.Job {
	src := `import fastapi
from pydantic import BaseModel

app = fastapi.FastAPI()

class UserRequest(BaseModel):
    name: str

def create_user(req: UserRequest):
    return {"name": req.name}

def health():
    return {"status": "ok"}
`
	return domain.Job{
		Key:   []byte("repo-abc-py"),
		Value: []byte(src),
		Topic: "raw-documents",
		Headers: map[string]string{
			"file_path":   "app/main.py",
			"source_type": "source_code",
		},
	}
}

func TestASTForwardingProcessor_GoSourceFile(t *testing.T) {
	prod := &fakeProducer{}
	p := processor.NewASTForwardingProcessor(prod, "graphrag.ast-parsed")
	job := goSourceJob()

	if err := p.Process(context.Background(), job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(prod.produced) != 1 {
		t.Fatalf("expected 1 produced record, got %d", len(prod.produced))
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(prod.produced[0].Value, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if payload["language"] != "go" {
		t.Errorf("language = %v, want go", payload["language"])
	}
	if payload["file_path"] != "cmd/server/main.go" {
		t.Errorf("file_path = %v, want cmd/server/main.go", payload["file_path"])
	}
	if payload["package_name"] != "main" {
		t.Errorf("package_name = %v, want main", payload["package_name"])
	}

	funcs, ok := payload["functions"].([]interface{})
	if !ok || len(funcs) == 0 {
		t.Fatal("expected non-empty functions list")
	}

	funcNames := make(map[string]bool)
	for _, f := range funcs {
		fm := f.(map[string]interface{})
		funcNames[fm["name"].(string)] = true
	}
	if !funcNames["main"] {
		t.Error("expected 'main' in functions")
	}
	if !funcNames["HandleHealth"] {
		t.Error("expected 'HandleHealth' in functions")
	}

	imports, ok := payload["imports"].([]interface{})
	if !ok || len(imports) == 0 {
		t.Fatal("expected non-empty imports list")
	}

	importSet := make(map[string]bool)
	for _, imp := range imports {
		importSet[imp.(string)] = true
	}
	if !importSet["fmt"] {
		t.Error("expected 'fmt' in imports")
	}
	if !importSet["net/http"] {
		t.Error("expected 'net/http' in imports")
	}
}

func TestASTForwardingProcessor_PythonSourceFile(t *testing.T) {
	prod := &fakeProducer{}
	p := processor.NewASTForwardingProcessor(prod, "graphrag.ast-parsed")
	job := pythonSourceJob()

	if err := p.Process(context.Background(), job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(prod.produced) != 1 {
		t.Fatalf("expected 1 produced record, got %d", len(prod.produced))
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(prod.produced[0].Value, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if payload["language"] != "python" {
		t.Errorf("language = %v, want python", payload["language"])
	}
	if payload["file_path"] != "app/main.py" {
		t.Errorf("file_path = %v, want app/main.py", payload["file_path"])
	}

	funcs, ok := payload["functions"].([]interface{})
	if !ok || len(funcs) == 0 {
		t.Fatal("expected non-empty functions list for Python file")
	}

	funcNames := make(map[string]bool)
	for _, f := range funcs {
		fm := f.(map[string]interface{})
		funcNames[fm["name"].(string)] = true
	}
	if !funcNames["UserRequest"] {
		t.Error("expected 'UserRequest' class in functions")
	}
	if !funcNames["create_user"] {
		t.Error("expected 'create_user' in functions")
	}

	imports, ok := payload["imports"].([]interface{})
	if !ok || len(imports) == 0 {
		t.Fatal("expected non-empty imports list for Python file")
	}

	importSet := make(map[string]bool)
	for _, imp := range imports {
		importSet[imp.(string)] = true
	}
	if !importSet["fastapi"] {
		t.Error("expected 'fastapi' in imports")
	}
	if !importSet["pydantic"] {
		t.Error("expected 'pydantic' in imports")
	}
}

func TestASTForwardingProcessor_ForwardsToParsedTopic(t *testing.T) {
	prod := &fakeProducer{}
	topic := "graphrag.ast-parsed"
	p := processor.NewASTForwardingProcessor(prod, topic)

	if err := p.Process(context.Background(), goSourceJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rec := prod.produced[0]
	if rec.Topic != topic {
		t.Errorf("topic = %q, want %q", rec.Topic, topic)
	}
	if rec.Headers["file_path"] != "cmd/server/main.go" {
		t.Errorf("header file_path = %q, want cmd/server/main.go", rec.Headers["file_path"])
	}
	if rec.Headers["language"] != "go" {
		t.Errorf("header language = %q, want go", rec.Headers["language"])
	}
}

func TestASTForwardingProcessor_UnsupportedExtensionSkipped(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
	}{
		{"yaml file", "k8s/deployment.yaml"},
		{"json file", "config/settings.json"},
		{"markdown", "docs/README.md"},
		{"toml config", "pyproject.toml"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prod := &fakeProducer{}
			p := processor.NewASTForwardingProcessor(prod, "graphrag.ast-parsed")
			job := domain.Job{
				Key:   []byte("key"),
				Value: []byte("some content"),
				Headers: map[string]string{
					"file_path":   tt.filePath,
					"source_type": "config",
				},
			}

			err := p.Process(context.Background(), job)
			if err != nil {
				t.Fatalf("unsupported extension should not error, got: %v", err)
			}
			if len(prod.produced) != 0 {
				t.Errorf("expected 0 produced records for unsupported extension, got %d", len(prod.produced))
			}
		})
	}
}

func TestASTForwardingProcessor_MissingFilePath(t *testing.T) {
	prod := &fakeProducer{}
	p := processor.NewASTForwardingProcessor(prod, "graphrag.ast-parsed")
	job := domain.Job{
		Key:     []byte("key"),
		Value:   []byte("content"),
		Headers: map[string]string{"source_type": "code"},
	}

	err := p.Process(context.Background(), job)
	if err == nil {
		t.Fatal("expected error for missing file_path header")
	}
}
