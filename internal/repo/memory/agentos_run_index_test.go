package memory

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestAgentOSRunIndexRejectsRunOwnershipOverwrite(t *testing.T) {
	index := NewAgentOSRunIndex()
	spec := agentos.RunSpec{
		RunID:          "run-1",
		IdempotencyKey: "run-start-1",
		Backend:        agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}
	if err := index.Bind(t.Context(), spec); err != nil {
		t.Fatalf("Bind first: %v", err)
	}
	if err := index.Bind(t.Context(), spec); err != nil {
		t.Fatalf("Bind replay: %v", err)
	}

	spec.Backend = agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "http-agent"}
	if err := index.Bind(t.Context(), spec); !errors.Is(err, agentos.ErrInvalidBackendRef) {
		t.Fatalf("Bind changed backend error = %v, want ErrInvalidBackendRef", err)
	}
}

func TestAgentOSRunIndexRequiresIdempotencyKey(t *testing.T) {
	index := NewAgentOSRunIndex()
	err := index.Bind(t.Context(), agentos.RunSpec{
		RunID:   "run-1",
		Backend: agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	})
	if !errors.Is(err, agentos.ErrInvalidRunSpec) {
		t.Fatalf("Bind error = %v, want ErrInvalidRunSpec", err)
	}
}
