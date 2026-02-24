package parser

import (
	"strings"
	"testing"
)

func TestParseGoFile_ExtractsPackageName(t *testing.T) {
	p := New()
	content := []byte(`package mypkg

func Foo() {}
`)
	result, err := p.ParseGoFile("foo.go", content)
	if err != nil {
		t.Fatalf("ParseGoFile: %v", err)
	}
	if result.PackageName != "mypkg" {
		t.Errorf("PackageName = %q; want mypkg", result.PackageName)
	}
}

func TestParseGoFile_ExtractsExportedFunctions(t *testing.T) {
	p := New()
	content := []byte(`package main

func ExportedFunc() {}
func unexportedFunc() {}
func AnotherExported() {}
`)
	result, err := p.ParseGoFile("foo.go", content)
	if err != nil {
		t.Fatalf("ParseGoFile: %v", err)
	}
	want := []string{"ExportedFunc", "AnotherExported"}
	if len(result.Functions) != len(want) {
		t.Errorf("Functions = %v; want %v", result.Functions, want)
	}
	for _, w := range want {
		found := false
		for _, f := range result.Functions {
			if f == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Functions missing %q; got %v", w, result.Functions)
		}
	}
}

func TestParseGoFile_ExtractsImports(t *testing.T) {
	p := New()
	content := []byte(`package main

import (
	"fmt"
	"strings"
	"net/http"
)
`)
	result, err := p.ParseGoFile("foo.go", content)
	if err != nil {
		t.Fatalf("ParseGoFile: %v", err)
	}
	want := []string{"fmt", "strings", "net/http"}
	for _, w := range want {
		found := false
		for _, imp := range result.Imports {
			if imp == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Imports missing %q; got %v", w, result.Imports)
		}
	}
}

func TestParseGoFile_IdentifiesHTTPHandlers(t *testing.T) {
	p := New()
	content := []byte(`package main

import "net/http"

func HandleIndex(w http.ResponseWriter, r *http.Request) {}
func HandleHealth(w http.ResponseWriter, r *http.Request) {}
func NotAHandler(x int, y string) {}
`)
	result, err := p.ParseGoFile("foo.go", content)
	if err != nil {
		t.Fatalf("ParseGoFile: %v", err)
	}
	wantHandlers := []string{"HandleIndex", "HandleHealth"}
	for _, w := range wantHandlers {
		found := false
		for _, h := range result.HTTPHandlers {
			if h == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("HTTPHandlers missing %q; got %v", w, result.HTTPHandlers)
		}
	}
	if len(result.HTTPHandlers) != 2 {
		t.Errorf("HTTPHandlers len = %d; want 2; got %v", len(result.HTTPHandlers), result.HTTPHandlers)
	}
}

func TestParsePythonFile_ExtractsClassesAndFunctions(t *testing.T) {
	p := New()
	content := []byte(`import os

class MyClass:
    pass

def my_func():
    pass

from foo import bar
`)
	result, err := p.ParsePythonFile("foo.py", content)
	if err != nil {
		t.Fatalf("ParsePythonFile: %v", err)
	}
	wantInFunctions := []string{"MyClass", "my_func"}
	for _, w := range wantInFunctions {
		found := false
		for _, f := range result.Functions {
			if f == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Functions missing %q; got %v", w, result.Functions)
		}
	}
	wantImports := []string{"os", "foo"}
	for _, w := range wantImports {
		found := false
		for _, imp := range result.Imports {
			if imp == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Imports missing %q; got %v", w, result.Imports)
		}
	}
}

func TestParseGoFile_InvalidSyntaxReturnsError(t *testing.T) {
	p := New()
	content := []byte(`package main

func broken(
`)
	result, err := p.ParseGoFile("foo.go", content)
	if err == nil {
		t.Fatalf("ParseGoFile: expected error for invalid syntax; got result %+v", result)
	}
	if !strings.Contains(err.Error(), "parse") {
		t.Errorf("error should mention parse; got %q", err.Error())
	}
}
