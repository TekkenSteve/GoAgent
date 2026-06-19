package agentosplan

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestCELCompilerRejectsExternalAndNondeterministicFunctions(t *testing.T) {
	compiler, err := NewCELCompiler()
	if err != nil {
		t.Fatalf("NewCELCompiler: %v", err)
	}

	expressions := []string{
		"now()",
		"random()",
		"http.get('https://example.com')",
		"readFile('/tmp/plan')",
	}
	for _, expression := range expressions {
		t.Run(expression, func(t *testing.T) {
			_, err := compiler.CompileValue(expression)
			if !errors.Is(err, agentos.ErrInvalidExpression) {
				t.Fatalf("CompileValue error = %v, want ErrInvalidExpression", err)
			}
		})
	}
}
