package stream

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

func TestPlanEventStreamDoesNotDependOnNativeStreamEvent(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "plan_event_stream.go", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse plan_event_stream.go imports: %v", err)
	}
	for _, imported := range file.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			t.Fatalf("unquote import path: %v", err)
		}
		if path == "github.com/TekkenSteve/GoAgent/internal/entity" {
			t.Fatalf("plan event stream must use public agentos.PlanEvent, not %s", path)
		}
	}

	file, err = parser.ParseFile(token.NewFileSet(), "plan_event_stream.go", nil, 0)
	if err != nil {
		t.Fatalf("parse plan_event_stream.go: %v", err)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "StreamEvent" {
			t.Fatal("plan event stream must not reference native StreamEvent")
		}

		return true
	})
}
