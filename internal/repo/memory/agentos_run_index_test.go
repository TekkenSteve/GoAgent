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

func TestAgentOSRunIndexRejectsRunIDWithDifferentIdempotencyKey(t *testing.T) {
	index := NewAgentOSRunIndex()
	spec := agentos.RunSpec{
		RunID:          "run-1",
		IdempotencyKey: "run-start-1",
		Backend:        agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}
	if err := index.Bind(t.Context(), spec); err != nil {
		t.Fatalf("Bind first: %v", err)
	}

	spec.IdempotencyKey = "run-start-2"
	if err := index.Bind(t.Context(), spec); !errors.Is(err, agentos.ErrInvalidRunSpec) {
		t.Fatalf("Bind changed key error = %v, want ErrInvalidRunSpec", err)
	}
}

func TestAgentOSRunIndexRejectsIdempotencyKeyWithDifferentRunID(t *testing.T) {
	index := NewAgentOSRunIndex()
	spec := agentos.RunSpec{
		RunID:          "run-1",
		IdempotencyKey: "run-start-1",
		Backend:        agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}
	if err := index.Bind(t.Context(), spec); err != nil {
		t.Fatalf("Bind first: %v", err)
	}

	spec.RunID = "run-2"
	if err := index.Bind(t.Context(), spec); !errors.Is(err, agentos.ErrInvalidRunSpec) {
		t.Fatalf("Bind reused key error = %v, want ErrInvalidRunSpec", err)
	}
}

func TestAgentOSRunIndexRejectsPlanNodeOwnershipChange(t *testing.T) {
	index := NewAgentOSRunIndex()
	spec := agentos.RunSpec{
		RunID:          "run-1",
		IdempotencyKey: "node-start-1",
		Backend:        agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}
	status := agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running"}
	if err := index.BindPlanNode(t.Context(), "plan-1", "node-1", spec, status); err != nil {
		t.Fatalf("BindPlanNode first: %v", err)
	}
	if err := index.BindPlanNode(t.Context(), "plan-1", "node-1", spec, status); err != nil {
		t.Fatalf("BindPlanNode replay: %v", err)
	}

	if err := index.BindPlanNode(t.Context(), "plan-1", "node-2", spec, status); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("BindPlanNode changed node error = %v, want ErrInvalidRunPlan", err)
	}
}
