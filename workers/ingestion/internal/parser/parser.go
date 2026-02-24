package parser

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strings"
)

// ParseResult holds the extracted entities from a parsed source file.
type ParseResult struct {
	PackageName string
	Functions   []string
	Imports     []string
	HTTPHandlers []string
}

// Parser provides file parsing for Go and Python source code.
type Parser struct{}

// New returns a new Parser.
func New() *Parser {
	return &Parser{}
}

// ParseGoFile parses a Go source file and extracts package name, exported functions,
// import paths, and HTTP handler functions.
func (p *Parser) ParseGoFile(filename string, content []byte) (*ParseResult, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, content, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("parse go file: %w", err)
	}

	result := &ParseResult{
		PackageName:  f.Name.Name,
		Functions:    make([]string, 0),
		Imports:      make([]string, 0),
		HTTPHandlers: make([]string, 0),
	}

	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.ImportSpec:
			path := importPath(x)
			if path != "" {
				result.Imports = append(result.Imports, path)
			}
			return true
		case *ast.FuncDecl:
			if x.Recv != nil {
				return true
			}
			name := x.Name.Name
			if !ast.IsExported(name) {
				return true
			}
			result.Functions = append(result.Functions, name)
			if isHTTPHandler(x.Type) {
				result.HTTPHandlers = append(result.HTTPHandlers, name)
			}
			return true
		default:
			return true
		}
	})

	return result, nil
}

func importPath(spec *ast.ImportSpec) string {
	if spec.Path == nil {
		return ""
	}
	s := spec.Path.Value
	s = strings.Trim(s, `"`)
	if spec.Name != nil {
		return spec.Name.Name + " " + s
	}
	return s
}

func isHTTPHandler(ft *ast.FuncType) bool {
	if ft.Params == nil || len(ft.Params.List) < 2 {
		return false
	}
	params := ft.Params.List
	first := paramTypeString(params[0])
	second := paramTypeString(params[1])
	return first == "http.ResponseWriter" && second == "*http.Request"
}

func paramTypeString(f *ast.Field) string {
	if len(f.Names) == 0 {
		return typeExprString(f.Type)
	}
	return typeExprString(f.Type)
}

func typeExprString(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return "*" + typeExprString(t.X)
	case *ast.SelectorExpr:
		pkg := typeExprString(t.X)
		return pkg + "." + t.Sel.Name
	default:
		return ""
	}
}

// ParsePythonFile parses a Python source file using regex-based extraction.
func (p *Parser) ParsePythonFile(filename string, content []byte) (*ParseResult, error) {
	contentStr := string(content)
	lines := strings.Split(contentStr, "\n")

	result := &ParseResult{
		PackageName:  "",
		Functions:    make([]string, 0),
		Imports:      make([]string, 0),
		HTTPHandlers: make([]string, 0),
	}

	classRe := regexp.MustCompile(`^\s*class\s+(\w+)`)
	defRe := regexp.MustCompile(`^\s*def\s+(\w+)`)
	importRe := regexp.MustCompile(`^\s*import\s+(\w+)`)
	fromRe := regexp.MustCompile(`^\s*from\s+(\w+)`)

	seenFuncs := make(map[string]struct{})
	seenImports := make(map[string]struct{})

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if m := classRe.FindStringSubmatch(line); m != nil {
			result.Functions = append(result.Functions, m[1])
			continue
		}
		if m := defRe.FindStringSubmatch(line); m != nil {
			if _, ok := seenFuncs[m[1]]; !ok {
				seenFuncs[m[1]] = struct{}{}
				result.Functions = append(result.Functions, m[1])
			}
			continue
		}
		if m := importRe.FindStringSubmatch(line); m != nil {
			if _, ok := seenImports[m[1]]; !ok {
				seenImports[m[1]] = struct{}{}
				result.Imports = append(result.Imports, m[1])
			}
			continue
		}
		if m := fromRe.FindStringSubmatch(line); m != nil {
			if _, ok := seenImports[m[1]]; !ok {
				seenImports[m[1]] = struct{}{}
				result.Imports = append(result.Imports, m[1])
			}
			continue
		}
	}

	return result, nil
}
