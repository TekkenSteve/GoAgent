package agentosplan

import (
	"context"
	"errors"
	"testing"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestCELCompilerRejectsNonDeterministicAndExternalCapabilities(t *testing.T) {
	t.Parallel()

	compiler, err := NewCELCompiler()
	if err != nil {
		t.Fatalf("NewCELCompiler: %v", err)
	}

	for _, expression := range []string{
		`timestamp("2026-06-19T00:00:00Z")`,
		`duration("1s")`,
		`now()`,
		`random()`,
		`http.get("https://example.invalid")`,
		`read_file("/tmp/input")`,
	} {
		t.Run(expression, func(t *testing.T) {
			t.Parallel()

			_, err := compiler.CompileValue(expression)
			if !errors.Is(err, agentoscore.ErrInvalidExpression) {
				t.Fatalf("CompileValue error = %v, want ErrInvalidExpression", err)
			}
		})
	}
}

func TestCELCompilerAllowsDeterministicPlanExpressions(t *testing.T) {
	t.Parallel()

	compiler, err := NewCELCompiler()
	if err != nil {
		t.Fatalf("NewCELCompiler: %v", err)
	}

	expression, err := compiler.Compile(`inputs.enabled && metadata.route == "fast" && size(artifacts) == 0`)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	ok, err := expression.Evaluate(context.Background(), map[string]any{
		"inputs":    map[string]any{"enabled": true},
		"metadata":  map[string]any{"route": "fast"},
		"artifacts": map[string]any{},
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	if !ok {
		t.Fatal("expression evaluated false")
	}
}
