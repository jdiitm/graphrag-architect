package processor

import (
	"testing"
)

func TestExtractGoAST(t *testing.T) {
	src := `package myservice

import (
	"net/http"
	"google.golang.org/grpc"
)

func Start() {
	http.Get("http://auth-service:8080/verify")
}
`
	result, err := ExtractGoAST("myservice/main.go", src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.PackageName != "myservice" {
		t.Errorf("expected package 'myservice', got %q", result.PackageName)
	}
	if len(result.Imports) != 2 {
		t.Errorf("expected 2 imports, got %d", len(result.Imports))
	}
	if len(result.ServiceHints) != 2 {
		t.Errorf("expected 2 service hints (http-server, grpc-server), got %d", len(result.ServiceHints))
	}
	if len(result.Functions) != 1 {
		t.Errorf("expected 1 function, got %d", len(result.Functions))
	}
	if len(result.HTTPCalls) != 1 {
		t.Errorf("expected 1 HTTP call, got %d", len(result.HTTPCalls))
	}
}

func TestExtractGoASTInvalidSyntax(t *testing.T) {
	_, err := ExtractGoAST("bad.go", "not valid go")
	if err == nil {
		t.Fatal("expected error for invalid Go syntax")
	}
}
